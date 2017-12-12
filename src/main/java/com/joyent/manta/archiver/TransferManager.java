/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.joyent.manta.client.MantaClient;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final int WAIT_MILLIS_FOR_COMPLETION_CHECK = 1000;

    private final TransferClient client;
    private final Path localRoot;

    /**
     * Creates a new instance backed by a transfer client mapped to a remote
     * filesystem root path and a local filesystem root path.
     *
     * @param client client used to transfer files
     * @param localRoot local filesystem working directory
     */
    public TransferManager(final TransferClient client, final Path localRoot) {
        this.client = client;

        if (localRoot != null) {
            this.localRoot = localRoot.toAbsolutePath().normalize();
        } else {
            this.localRoot = null;
        }
    }

    /**
     * Uploads all the files from the local working directory to the remote
     * working directory. It won't upload files that are identical, but it will
     * overwrite files that are different.
     *
     * @throws InterruptedException thrown when a blocking operation is interrupted
     */
    @SuppressWarnings("EmptyStatement")
    void uploadAll() throws InterruptedException {
        final ForkJoinPool loaderPool = new ForkJoinPool(
                ForkJoinPool.getCommonPoolParallelism(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                new LoggingUncaughtExceptionHandler("ObjectCompressorThreadPool"),
                true);

        final int concurrentUploaders = Math.max(client.getMaximumConcurrentConnections() - 2, 1);
        final ExecutorService uploaderExecutor = Executors.newFixedThreadPool(
                concurrentUploaders, new NamedThreadFactory(
                        "uploader-thread-%d", "uploaders",
                        "UploaderThreadPool"));

        final int noOfinitialAsyncObjectsToProcess = concurrentUploaders * 2;
        final ObjectUploadQueueLoader loader = new ObjectUploadQueueLoader(loaderPool,
                noOfinitialAsyncObjectsToProcess);
        final TransferQueue<ObjectUpload> queue = loader.getQueue();

        final TotalTransferDetails transferDetails = loader.uploadDirectoryContents(localRoot);
        final long noOfObjectToUpload = transferDetails.numberOfFiles;

        if (noOfObjectToUpload < 1) {
            return;
        }

        System.err.println("Maven Archiver - Upload");
        System.err.println();

        System.err.printf("Bulk upload to Manta : [%s] --> [%s]%s",
                localRoot, client.getRemotePath(), System.lineSeparator());
        System.err.printf("Total files to upload: %d%s", transferDetails.numberOfFiles,
                System.lineSeparator());
        System.err.printf("Total size to upload : %s (%d)%s",
                FileUtils.byteCountToDisplaySize(transferDetails.numberOfBytes),
                transferDetails.numberOfBytes, System.lineSeparator());

        System.err.println();

        final String uploadMsg = "Uploading";
        final ProgressBar pb = new ProgressBar(uploadMsg,
                transferDetails.numberOfBytes, ProgressBarStyle.ASCII);

        final AtomicLong totalUploads = new AtomicLong();
        final Callable<Long> uploader = new ObjectUploadCallable(totalUploads,
                queue, noOfObjectToUpload, client, localRoot, pb);

        final List<Callable<Long>> uploaders = Stream.generate(() -> uploader)
                .limit(concurrentUploaders).collect(Collectors.toList());

        pb.start();

        final List<Future<Long>> futures = uploaders.stream()
                .map(uploaderExecutor::submit).collect(Collectors.toList());

        while (!futures.stream().allMatch(Future::isDone)) {
            Thread.sleep(WAIT_MILLIS_FOR_COMPLETION_CHECK);
        }
        LOG.debug("All futures have completed");

        if (LOG.isDebugEnabled()) {
            final long totalProcessed = sumOfAllFilesProcessed(futures);
            LOG.debug("Total actually processed: {}", totalProcessed);
        }

        // This is where we block waiting for the uploaders to finish
        while (totalUploads.get() < transferDetails.numberOfFiles) {
            Thread.sleep(WAIT_MILLIS_FOR_COMPLETION_CHECK);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("All uploads [{}/{}] have completed", totalUploads.get(),
                    noOfObjectToUpload);
        }

        LOG.debug("Shutting down object compression thread pool");
        loaderPool.shutdown();
        while (!loaderPool.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS));

        shutdownUploads(uploaderExecutor);
        pb.stop();
    }

    /**
     * Downloads all files from a remote Manta path into the local working
     * directory.
     *
     * @throws InterruptedException thrown when a blocking operation is interrupted
     */
    void downloadAll() throws InterruptedException {
        final int concurrentDownloaders = Math.max(client.getMaximumConcurrentConnections() - 2, 1);
        final ExecutorService downloadExecutor = Executors.newFixedThreadPool(
                concurrentDownloaders, new NamedThreadFactory(
                        "download-thread-%d", "downloaders",
                        "DownloaderThreadPool"));

        System.err.println("Maven Archiver - Download");
        System.err.println();

        final String format = "[%s] %s --> %s" + System.lineSeparator();

        final AtomicBoolean verificationSuccess = new AtomicBoolean(true);
        final AtomicLong totalObjects = new AtomicLong(0L);
        final AtomicLong totalObjectsProcessed = new AtomicLong(0L);

        try (Stream<String> objects = client.find()) {
            objects.forEach(remoteObject -> {
                final Path path = client.convertRemotePathToLocalPath(remoteObject, localRoot);
                totalObjects.incrementAndGet();

                if (remoteObject.endsWith(MantaClient.SEPARATOR)) {
                    path.toFile().mkdirs();
                    totalObjectsProcessed.incrementAndGet();
                } else {
                    final Path parent = path.getParent();
                    if (!parent.toFile().exists()) {
                        parent.toFile().mkdirs();
                    }

                    final Callable<Void> download = () -> {
                        try (OutputStream out = Files.newOutputStream(path)) {
                            final VerificationResult result = client.download(remoteObject, out);

                            if (verificationSuccess.get() && !result.equals(VerificationResult.OK)) {
                                verificationSuccess.set(false);
                            }

                            totalObjectsProcessed.incrementAndGet();
                            System.err.printf(format, result, remoteObject, path);
                        }

                        return null;
                    };

                    downloadExecutor.submit(download);
                }
            });
        }

        downloadExecutor.shutdown();

        while (totalObjectsProcessed.get() != totalObjects.get()) {
            downloadExecutor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS);
        }
    }

    /**
     * Verifies that all of the files in the specified local directory
     * and subdirectories are identical to the files on Manta.
     *
     * @return true when all files verified successfully
     * @throws InterruptedException thrown when a blocking operation is interrupted
     */
    boolean verifyLocal() throws InterruptedException {
        System.err.println("Maven Archiver - Verify Local");
        System.err.println();

        final AtomicBoolean verificationSuccess = new AtomicBoolean(true);
        final int statusMsgSize = 19;

        final String format = "[%s] %s <-> %s" + System.lineSeparator();

        try (Stream<Path> contents = LocalFileUtils.directoryContentsStream(localRoot)) {
            contents.forEach(localPath -> {
                String mantaPath = client.convertLocalPathToRemotePath(localPath, localRoot);

                final VerificationResult result;

                if (localPath.toFile().isDirectory()) {
                    result = client.verifyDirectory(mantaPath);
                } else {
                    final File file = localPath.toFile();
                    final byte[] checksum = LocalFileUtils.checksum(localPath);
                    result = client.verifyFile(mantaPath, file.length(), checksum);
                }

                if (verificationSuccess.get() && !result.equals(VerificationResult.OK)) {
                    verificationSuccess.set(false);
                }

                System.err.printf(format, StringUtils.center(result.toString(), statusMsgSize),
                        localPath, mantaPath);
            });
        }

        return verificationSuccess.get();
    }

    /**
     * Verifies that all of the files in the specified remote directory
     * and subdirectories are identical to the files on Manta.
     *
     * @return true when all files verified successfully
     * @throws InterruptedException thrown when a blocking operation is interrupted
     */
    boolean verifyRemote() throws InterruptedException {
        final int concurrentVerifiers = Math.max(client.getMaximumConcurrentConnections() - 2, 1);
        final ExecutorService verifyExecutor = Executors.newFixedThreadPool(
                concurrentVerifiers, new NamedThreadFactory(
                        "verify-thread-%d", "verifiers",
                        "VerifierThreadPool"));

        System.err.println("Maven Archiver - Verify Remote");
        System.err.println();

        final AtomicBoolean verificationSuccess = new AtomicBoolean(true);
        final int statusMsgSize = 19;

        final String format = "[%s] %s" + System.lineSeparator();

        final AtomicLong totalFiles = new AtomicLong(0L);
        final AtomicLong totalFilesProcessed = new AtomicLong(0L);

        try (Stream<String> files = client.find();
             OutputStream out = new NullOutputStream()) {
            // Only process files and no directories
            files.filter(p -> !p.endsWith(MantaClient.SEPARATOR)).forEach(mantaPath -> {
                totalFiles.incrementAndGet();

                final Callable<Void> verify = () -> {
                    final VerificationResult result = client.download(mantaPath, out);

                    if (verificationSuccess.get() && !result.equals(VerificationResult.OK)) {
                        verificationSuccess.set(false);
                    }

                    System.err.printf(format, StringUtils.center(result.toString(), statusMsgSize),
                            mantaPath);

                    totalFilesProcessed.incrementAndGet();
                    return null;
                };

                verifyExecutor.submit(verify);
            });
        } catch (IOException e) {
            String msg = "Unable to verify remote files";
            TransferClientException tce = new TransferClientException(msg, e);
            throw tce;
        }

        verifyExecutor.shutdown();

        while (totalFilesProcessed.get() != totalFiles.get()) {
            verifyExecutor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS);
        }

        System.err.printf("%d/%d files verified%s", totalFilesProcessed.get(), totalFiles.get(),
                System.lineSeparator());

        return verificationSuccess.get();
    }

    private static long sumOfAllFilesProcessed(final Collection<Future<Long>> futures) {
        return futures.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new FileProcessingException(e);
            }
        }).mapToLong(Long::longValue).sum();
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
    }
}
