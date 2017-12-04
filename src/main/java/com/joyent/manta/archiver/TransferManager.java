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

import java.nio.file.Path;
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

    private static final int EXECUTOR_SHUTDOWN_WAIT_SECS = 5;

    private final ForkJoinPool loaderPool = new ForkJoinPool(
            ForkJoinPool.getCommonPoolParallelism(),
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            new LoggingUncaughtExceptionHandler("ObjectCompressorThreadPool"),
            true);

    private final TransferClient client;
    private final Path localRoot;
    private final String remoteRoot;

    /**
     * Creates a new instance backed by a transfer client mapped to a remote
     * filesystem root path and a local filesystem root path.
     *
     * @param client client used to transfer files
     * @param localRoot local filesystem working directory
     * @param remoteRoot remote filesystem working directory
     */
    public TransferManager(final TransferClient client, final Path localRoot,
                           final String remoteRoot) {
        this.client = client;
        this.localRoot = localRoot.toAbsolutePath().normalize();

        String normalized = FilenameUtils.normalize(remoteRoot, true);

        if (normalized.endsWith(MantaClient.SEPARATOR)) {
            this.remoteRoot = normalized;
        } else {
            this.remoteRoot = normalized + MantaClient.SEPARATOR;
        }
    }

    /**
     * Uploads all the files from the local working directory to the remote
     * working directory. It won't upload files that are identical, but it will
     * overwrite files that are different.
     *
     * @throws InterruptedException thrown when a blocking operation is interrupted
     */
    void uploadAll() throws InterruptedException {
        final int concurrentUploaders = Math.max(client.getMaximumConcurrentUploads() - 2, 1);
        final ExecutorService uploaderExecutor = Executors.newFixedThreadPool(
                concurrentUploaders, new UploaderThreadFactory());

        final int noOfinitialAsyncObjectsToProcess = concurrentUploaders * 2;
        final ObjectUploadQueueLoader loader = new ObjectUploadQueueLoader(loaderPool,
                noOfinitialAsyncObjectsToProcess);
        final TransferQueue<ObjectUpload> queue = loader.getQueue();

        final long noOfObjectToUpload = loader.uploadDirectoryContents(localRoot);

        if (noOfObjectToUpload < 1) {
            return;
        }

        final AtomicLong totalUploads = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch(concurrentUploaders);
        final Callable<Void> uploader = new ObjectUploadCallable(totalUploads,
                latch, queue, noOfObjectToUpload, client, localRoot);

        final List<Callable<Void>> uploaders = Stream.generate(() -> uploader)
                .limit(concurrentUploaders).collect(Collectors.toList());
        uploaderExecutor.invokeAll(uploaders);

        // This is where we block waiting for the uploaders to finish
        latch.await();

        if (LOG.isInfoEnabled()) {
            LOG.info("All uploads [{}/{}] have completed", totalUploads.get(),
                    noOfObjectToUpload);
        }

        shutdownUploads(uploaderExecutor);
    }

    @SuppressWarnings("EmptyStatement")
    private void shutdownUploads(final ExecutorService uploaderExecutor)
            throws InterruptedException {
        LOG.debug("Shutting down uploader thread pool");
        uploaderExecutor.shutdown();
        while (!uploaderExecutor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS));

        if (!uploaderExecutor.isShutdown()) {
            uploaderExecutor.shutdownNow();
        }
    }

    @SuppressWarnings("EmptyStatement")
    @Override
    public void close() {
        client.close();

        LOG.debug("Shutting down object compression thread pool");
        loaderPool.shutdown();

        try {
            while (!loaderPool.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if (!loaderPool.isShutdown()) {
            loaderPool.shutdownNow();
        }
    }
}
