/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.joyent.manta.client.MantaClient;
import org.apache.commons.io.FilenameUtils;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

/**
 *
 */
public class TransferManager implements AutoCloseable {
    private final ForkJoinPool executor = new ForkJoinPool();
    private final ObjectUploadQueueLoader loader = new ObjectUploadQueueLoader(executor);
    private final TransferClient client;
    private final Path root;
    private final String mantaRoot;

    public TransferManager(final TransferClient client, final Path root,
                           final String mantaRoot) {
        this.client = client;
        this.root = root.toAbsolutePath().normalize();

        String normalized = FilenameUtils.normalize(mantaRoot, true);

        if (normalized.endsWith(MantaClient.SEPARATOR)) {
            this.mantaRoot = normalized;
        } else {
            this.mantaRoot = normalized + MantaClient.SEPARATOR;
        }
    }

    void uploadAll() {
        ForkJoinTask<?> task = executor.submit(() -> loader.processDirectoryContents(root));
        TransferQueue<ObjectUpload> queue = loader.getQueue();

        try {
            while (!task.isDone()) {
                final ObjectUpload upload = queue.poll(1, TimeUnit.SECONDS);

                if (upload == null) {
                    continue;
                }

                uploadObject(upload);
            }
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
        }
    }

    void uploadObject(final ObjectUpload upload) {
        if (upload.isDirectory()) {
            uploadDirectory((DirectoryUpload)upload);
        } else {
            uploadFile((FileUpload)upload);
        }
    }

    void uploadDirectory(final DirectoryUpload upload) {
        String mantaDir = convertToMantaPath(upload);

        client.mkdirp(mantaDir, upload);
    }

    void uploadFile(final FileUpload upload) {
        String mantaPath = convertToMantaPath(upload);

        client.put(mantaPath, upload);
    }

    String convertToMantaPath(final ObjectUpload upload) {
        Path sourcePath = upload.getSourcePath();
        Path subPath = root.relativize(sourcePath);

        StringBuilder builder = new StringBuilder(mantaRoot);

        Iterator<Path> itr = subPath.iterator();

        String filename = "";

        for (int parts = 0; itr.hasNext(); parts++) {
            filename = itr.next().getFileName().toString();

            if (filename.isEmpty()) {
                continue;
            }

            if (parts > 0) {
                builder.append(MantaClient.SEPARATOR);
            }

            builder.append(filename);
        }

        if (upload.isDirectory()
                && !filename.isEmpty()
                && !filename.endsWith(MantaClient.SEPARATOR)) {
            builder.append(MantaClient.SEPARATOR);
        } else {
            builder.append(".").append(ObjectCompressor.COMPRESSION_TYPE);
        }

        return builder.toString();
    }

    @Override
    public void close() {
        client.close();
        executor.shutdownNow();
    }
}
