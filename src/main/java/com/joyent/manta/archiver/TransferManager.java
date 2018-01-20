/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.google.common.cache.Cache;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Class responsible for managing the ingestion of the compressed file queue
 * and the allocation of uploader threads.
 */
public class TransferManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TransferManager.class);

    private static final int EXECUTOR_SHUTDOWN_WAIT_SECS = 5;

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

        final int preloadQueueSize = concurrentUploaders * 4;
        final ObjectUploadQueueLoader loader = new ObjectUploadQueueLoader(loaderPool,
                preloadQueueSize);
        final TransferQueue<ObjectUpload> queue = loader.getQueue();

        // We queue up the directory traversal and file processing work asynchronously
        // so that we can start uploading right away and we don't need to wait for the
        // entire recursive traversal to complete.
        Future<TotalTransferDetails> transferDetailsFuture = uploaderExecutor.submit(
                () -> loader.uploadDirectoryContents(localRoot));

        final AtomicReference<ProgressBar> pbRef = new AtomicReference<>();
        final AtomicLong totalUploads = new AtomicLong(0L);
        final AtomicLong noOfObjectToUpload = new AtomicLong(Long.MAX_VALUE);

        final Cache<String, Boolean> dirCache;
        if (client instanceof MantaTransferClient) {
            dirCache = ((MantaTransferClient) client).getDirCache();
        } else {
            dirCache = null;
        }

        UploadStatusFunction statusFunction = new UploadStatusFunction(
                transferDetailsFuture, totalUploads, noOfObjectToUpload,
                dirCache, queue, loaderPool);

        registerSighupFunction(statusFunction);

        final Runnable uploader = new ObjectUploadRunnable(totalUploads,
                queue, noOfObjectToUpload, client, localRoot, pbRef);

        // This starts all of the uploader threads
        for (int i = 0; i < concurrentUploaders; i++) {
            uploaderExecutor.execute(uploader);
        }

        final TotalTransferDetails transferDetails;

        try {
             transferDetails = transferDetailsFuture.get();
        } catch (ExecutionException e) {
            throw new FileProcessingException(e);
        }

        if (transferDetails.numberOfObjects < 1) {
            return;
        }

        noOfObjectToUpload.set(transferDetails.numberOfObjects);

        System.err.println("Maven Archiver - Upload");
        System.err.println();

        System.err.printf("Bulk upload to Manta : [%s] --> [%s]%s",
                localRoot, client.getRemotePath(), System.lineSeparator());
        System.err.printf("Total files to upload: %d%s", transferDetails.numberOfObjects,
                System.lineSeparator());
        System.err.printf("Total size to upload : %s (%d)%s",
                FileUtils.byteCountToDisplaySize(transferDetails.numberOfBytes),
                transferDetails.numberOfBytes, System.lineSeparator());

        System.err.println();

        final String uploadMsg = "Uploading";
        final ProgressBar pb = new ProgressBar(uploadMsg,
                transferDetails.numberOfBytes, ProgressBarStyle.ASCII);

        pb.start();

        pbRef.set(pb);

        LOG.debug("Shutting down object compression thread pool");
        loaderPool.shutdown();
        while (!loaderPool.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS)) {
            LOG.trace("Waiting for object preloader executor to shutdown");
        }

        LOG.debug("Shutting down object uploader thread pool");
        uploaderExecutor.shutdown();
        while (!uploaderExecutor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS)) {
            LOG.trace("Waiting for uploader executor to shutdown. Uploaded {}/{} objects. "
                            + "{} objects pre-processed.",
                    totalUploads.get(), noOfObjectToUpload, loader.getObjectsProcessed().get());
        }

        if (totalUploads.get() != noOfObjectToUpload.get()) {
            pb.stop();

            String msg = "Actual number of objects uploads differs from expected number";
            TransferClientException e = new TransferClientException(msg);
            e.setContextValue("expectedNumberOfUploads", noOfObjectToUpload);
            e.setContextValue("actualNumberOfUploads", totalUploads.get());
            throw e;
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("All uploads [{}] have completed", totalUploads.get(),
                    noOfObjectToUpload);
        }

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

        final AtomicBoolean verificationSuccess = new AtomicBoolean(true);
        final AtomicLong totalObjects = new AtomicLong(0L);
        final AtomicLong totalObjectsProcessed = new AtomicLong(0L);

        try (Stream<FileDownload> downloads = client.find()) {
            downloads.forEach(fileDownload -> {
                totalObjects.incrementAndGet();

                final Path path = client.convertRemotePathToLocalPath(fileDownload.getRemotePath(), localRoot);
                final File file = path.toFile();

                if (fileDownload.isDirectory()) {
                    if (file.exists() && file.lastModified() != fileDownload.getLastModified()) {
                        if (!file.setLastModified(fileDownload.getLastModified())) {
                            LOG.warn("Unable to set last modified time for directory: {}",
                                    file);
                        }
                    } else {
                        file.mkdirs();
                    }

                    totalObjectsProcessed.incrementAndGet();
                } else {
                    if (!file.exists()) {
                        final Path parent = path.getParent();
                        if (!parent.toFile().exists()) {
                            parent.toFile().mkdirs();
                        }
                    }

                    Runnable download = new ObjectDownloadRunnable(
                            path, client, fileDownload, verificationSuccess,
                            totalObjectsProcessed);
                    downloadExecutor.execute(download);
                }
            });
        }

        downloadExecutor.shutdown();

        while (totalObjectsProcessed.get() < totalObjects.get()) {
            downloadExecutor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS);
        }

        System.err.println();
        System.err.printf("Downloaded %d/%d objects%s",
                totalObjectsProcessed.get(), totalObjects.get(), System.lineSeparator());
    }

    /**
     * Verifies that all of the files in the specified local directory
     * and subdirectories are identical to the files on Manta.
     *
     * @param fix when true we upload any missing files
     * @return true when all files verified successfully
     */
    boolean verifyLocal(final boolean fix) {
        System.err.println("Maven Archiver - Verify Local");
        System.err.println();

        if (fix) {
            System.err.println("Running in verify fix mode. Missing or "
                    + "corrupted files will be uploaded");
            System.err.println();
        }

        final AtomicBoolean verificationSuccess = new AtomicBoolean(true);
        final int statusMsgSize = 27;

        final String format = "[%s] %s <-> %s" + System.lineSeparator();

        try (Stream<Path> contents = LocalFileUtils.directoryContentsStream(localRoot)) {
            contents.parallel().forEach(localPath -> {
                String mantaPath = client.convertLocalPathToRemotePath(localPath, localRoot);

                final VerificationResult result;

                if (Files.isSymbolicLink(localPath)) {
                    result = client.verifyLink(mantaPath, localPath);
                } else if (Files.isDirectory(localPath, LinkOption.NOFOLLOW_LINKS)) {
                    result = client.verifyDirectory(mantaPath);
                } else {
                    final long size;

                    try {
                        size = Files.size(localPath);
                    } catch (IOException e) {
                        String msg = String.format("Unable to get the size of "
                                + "local file: %s", localPath);
                        throw new UncheckedIOException(msg, e);
                    }

                    final byte[] checksum = LocalFileUtils.checksum(localPath);
                    result = client.verifyFile(mantaPath, size, checksum);
                }

                if (verificationSuccess.get() && !result.isOk()) {
                    verificationSuccess.set(false);
                }

                System.err.printf(format, StringUtils.center(result.toString(), statusMsgSize),
                        localPath, mantaPath);

                if (fix && !result.isOk()) {
                    System.err.printf(format, StringUtils.center("FIXING", statusMsgSize),
                            localPath, mantaPath);
                    if (Files.isSymbolicLink(localPath)) {
                        SymbolicLinkUpload upload = new SymbolicLinkUpload(localPath);

                        if (result.isNotLink()) {
                            boolean recursive = VerificationResult.NOT_LINK_ACTUALLY_DIR.equals(result);
                            client.delete(mantaPath, recursive);
                        }

                        client.put(mantaPath, upload);
                    } else if (Files.isDirectory(localPath, LinkOption.NOFOLLOW_LINKS)) {
                        client.mkdirp(mantaPath, new DirectoryUpload(localPath));
                    } else {
                        FileUpload upload = ObjectUploadQueueLoader.fileToUploadFromPath(localPath);
                        client.put(mantaPath, upload);

                        try {
                            Files.deleteIfExists(upload.getTempPath());
                        } catch (IOException e) {
                            LOG.error("Unable to delete temp file", e);
                        }
                    }
                }
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

        final String format = "[%s] %s" + System.lineSeparator();

        final AtomicLong totalFiles = new AtomicLong(0L);
        final AtomicLong totalFilesProcessed = new AtomicLong(0L);

        try (Stream<FileDownload> files = client.find()) {
            // Only process files (no links and no directories)
            files.filter(f -> !f.isDirectory() && !f.isLink()).forEach(file -> {
                totalFiles.incrementAndGet();

                final Runnable verify = () -> {
                    final VerificationResult result = client.download(
                            file.getRemotePath(), Optional.empty());

                    if (verificationSuccess.get() && !result.isOk()) {
                        verificationSuccess.set(false);
                    }

                    String centered = StringUtils.center(result.toString(), VerificationResult.MAX_STRING_SIZE);
                    System.err.printf(format, centered, file);

                    totalFilesProcessed.incrementAndGet();
                };

                verifyExecutor.execute(verify);
            });
        }

        verifyExecutor.shutdown();

        while (totalFilesProcessed.get() != totalFiles.get()) {
            verifyExecutor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS);
        }

        System.err.printf("%d/%d files verified%s", totalFilesProcessed.get(), totalFiles.get(),
                System.lineSeparator());

        return verificationSuccess.get();
    }

    @SuppressWarnings("EmptyStatement")
    @Override
    public void close() {
        client.close();
    }

    /**
     * Safely registers a SIGHUP handler that works for systems that support signals.
     * This seemingly complex way of loading the sun.misc.* classes is important because
     * it allows us to not explicitly depend on them thereby making this method safe to
     * run on any JVM.
     *
     * @param function function to run upon SIGHUP
     */
    public void registerSighupFunction(final Function<Void, Optional<RuntimeException>> function) {
        try {
            Class<?> signalClass = Class.forName("sun.misc.Signal");
            Class<?> signalHandlerClass = Class.forName("sun.misc.SignalHandler");
            Constructor<?> signalClassConstructor = signalClass.getConstructor(String.class);
            Method handle = signalClass.getMethod("handle", signalClass, signalHandlerClass);
            Object signalInstance = signalClassConstructor.newInstance("USR2");
            handle.invoke(null, signalInstance, new Sigusr2Handler(function));
        } catch (ReflectiveOperationException e) {
            final String msg = "Unable to register signal handler via reflection";
                LOG.warn(msg, e);
        }
    }
}
