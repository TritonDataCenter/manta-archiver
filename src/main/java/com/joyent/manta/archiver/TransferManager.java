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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class responsible for managing the ingestion of the compressed file queue
 * and the allocation of uploader threads.
 */
public class TransferManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TransferManager.class);
    private final ForkJoinPool loaderPool = new ForkJoinPool(
            ForkJoinPool.getCommonPoolParallelism(),
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            new LoggingUncaughtExceptionHandler("ObjectCompressorThreadPool"),
            true);

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

    void uploadAll() throws InterruptedException {
        final int concurrentUploaders = Math.max(client.getMaximumConcurrentUploads() - 2, 1);
        final ExecutorService uploaderExecutor = Executors.newFixedThreadPool(
                concurrentUploaders, new UploaderThreadFactory());

        final int noOfinitialAsyncObjectsToProcess = concurrentUploaders * 2;
        final ObjectUploadQueueLoader loader = new ObjectUploadQueueLoader(loaderPool,
                noOfinitialAsyncObjectsToProcess);
        final TransferQueue<ObjectUpload> queue = loader.getQueue();

        final long noOfObjectToUpload = loader.processDirectoryContents(root);

        if (noOfObjectToUpload < 1) {
            return;
        }

        final AtomicLong totalUploads = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch(concurrentUploaders);
        final Callable<Void> uploader = new ObjectUploadCallable(totalUploads,
                latch, queue, noOfObjectToUpload, this::uploadObject);

        final List<Callable<Void>> uploaders = Stream.generate(() -> uploader)
                .limit(concurrentUploaders).collect(Collectors.toList());
        uploaderExecutor.invokeAll(uploaders);

        // This is where we block waiting for the uploaders to finish
        latch.await();

        if (LOG.isInfoEnabled()) {
            LOG.info("All uploads [{}/{}] have completed", totalUploads.get(),
                    noOfObjectToUpload);
        }

        LOG.debug("Shutting down object compression thread pool");
        loaderPool.shutdown();
        while (!loaderPool.awaitTermination(5, TimeUnit.SECONDS));

        if (!loaderPool.isShutdown()) {
            loaderPool.shutdownNow();
        }

        LOG.debug("Shutting down uploader thread pool");
        uploaderExecutor.shutdown();
        while (!uploaderExecutor.awaitTermination(5, TimeUnit.SECONDS));

        if (!uploaderExecutor.isShutdown()) {
            uploaderExecutor.shutdownNow();
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

        // Clean up the temp upload file so we don't leave it lingering
        try {
            Files.deleteIfExists(upload.getTempPath());
        } catch (IOException e) {
            String msg = String.format("Unable to delete [%s]",
                    upload.getTempPath());
            LOG.warn(msg, e);
        }
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
        loaderPool.shutdownNow();
    }
}
