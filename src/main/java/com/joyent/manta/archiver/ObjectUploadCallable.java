/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link Callable} implementation that handles file uploads.
 */
class ObjectUploadCallable implements Callable<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectUploadCallable.class);

    private final AtomicLong totalUploads;
    private final CountDownLatch latch;
    private final TransferQueue<ObjectUpload> queue;
    private final long noOfObjectToUpload;
    private final TransferClient client;
    private final Path localRoot;
    private final ProgressBar pb;

    /**
     * Creates a new instance.
     *
     * @param totalUploads total number of completed uploads
     * @param latch count down to track when upload threads have finished
     * @param queue queue containing uploads
     * @param noOfObjectToUpload total number of objects to upload
     * @param client transfer client used to upload objects
     * @param localRoot local working directory
     * @param pb reference to progress bar to update
     */
    ObjectUploadCallable(final AtomicLong totalUploads,
                         final CountDownLatch latch,
                         final TransferQueue<ObjectUpload> queue,
                         final long noOfObjectToUpload,
                         final TransferClient client,
                         final Path localRoot,
                         final ProgressBar pb) {
        this.totalUploads = totalUploads;
        this.latch = latch;
        this.queue = queue;
        this.noOfObjectToUpload = noOfObjectToUpload;
        this.client = client;
        this.localRoot = localRoot;
        this.pb = pb;
    }

    @Override
    public Void call() throws Exception {
        try {
            while (totalUploads.get() < noOfObjectToUpload) {
                ObjectUpload upload = queue.poll(1, TimeUnit.SECONDS);

                if (upload == null) {
                    continue;
                }

                try {
                    uploadObject(upload);
                } catch (RuntimeException e) {
                    LOG.error("Error uploading file. Adding file back to the queue", e);
                    queue.put(upload);
                    continue;
                }

                /* After an upload is successful we want to know if we
                 * have uploaded all of the objects that we can upload
                 * because this will let us know when to exit the loop. */
                if (totalUploads.incrementAndGet() >= noOfObjectToUpload) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        latch.countDown();
        return null;
    }

    /**
     * Uploads an object to the remote data store.
     *
     * @param upload object to upload.
     */
    void uploadObject(final ObjectUpload upload) {
        if (upload.isDirectory()) {
            createDirectory((DirectoryUpload)upload);
        } else {
            uploadFile((FileUpload)upload);
        }
    }

    /**
     * Creates directory on the remote data store.
     * @param upload directory to create
     */
    void createDirectory(final DirectoryUpload upload) {
        String mantaDir = client.convertLocalPathToRemotePath(upload, localRoot);

        client.mkdirp(mantaDir, upload);
    }

    /**
     * Uploads a file to the remote data store.
     * @param upload file to upload
     */
    void uploadFile(final FileUpload upload) {
        String mantaPath = client.convertLocalPathToRemotePath(upload, localRoot);

        try {
            upload.incrementUploadAttempts();
            client.put(mantaPath, upload);
        } catch (RuntimeException e) {
            String msg = String.format("Error uploading file [%s] adding file "
                    + "back to the queue", upload.getSourcePath());
            LOG.warn(msg, e);

            try {
                queue.put(upload);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            return;
        }

        // Clean up the temp upload file so we don't leave it lingering
        try {
            Files.deleteIfExists(upload.getTempPath());
            pb.stepBy(upload.getUncompressedSize());

            if (LOG.isInfoEnabled()) {
                LOG.info("Upload [{} {}] has completed",
                        upload.getSourcePath(),
                        FileUtils.byteCountToDisplaySize(upload.getUncompressedSize()));
            }
        } catch (IOException e) {
            String msg = String.format("Unable to delete [%s]",
                    upload.getTempPath());
            LOG.warn(msg, e);
        }
    }
}
