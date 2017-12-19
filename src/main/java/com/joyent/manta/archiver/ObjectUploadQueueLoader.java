/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.twmacinta.util.FastMD5Digest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.io.DigestInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.io.File.separator;

/**
 * Class in charge of compressing files and doing checksums on files that will
 * be copied to Manta.
 */
class ObjectUploadQueueLoader {
    static final Path TEMP_PATH = Paths.get(System.getProperty("java.io.tmpdir")
            + separator + "manta-archiver");

    private static final Logger LOG = LoggerFactory.getLogger(ObjectUploadQueueLoader.class);

    private static final int FILE_READ_BUFFER = 16_384;

    private static final ObjectCompressor COMPRESSOR = new ObjectCompressor();

    private final ForkJoinPool executor;
    private final TransferQueue<ObjectUpload> queue;
    private final int queuePreloadSize;
    private final AtomicInteger preloadedCount = new AtomicInteger();
    private final AtomicLong objectsProcessed = new AtomicLong(0L);

    static {
        // Queue up deletion of temp files when process exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Deleting temp files");

            try {
                FileUtils.forceDelete(TEMP_PATH.toFile());
            } catch (IOException e) {
                LOG.warn("Unable to delete temp files from path: " + TEMP_PATH, e);
            }
        }));
    }

    /**
     * Creates a new queue loader based on the specified fork join pool
     * and the maximum queue preload size.
     *
     * @param executor fork join pool used for concurrent operations
     * @param queuePreloadSize number of objects to process before we start
     *                         blocking queue transfer operations
     */
    ObjectUploadQueueLoader(final ForkJoinPool executor, final int queuePreloadSize) {
        this.executor = executor;
        this.queuePreloadSize = queuePreloadSize;
        this.queue = new LinkedTransferQueue<>();

        boolean dataDirCreated = TEMP_PATH.toFile().mkdir();

        if (dataDirCreated) {
            LOG.info("Created new temporary data directory: {}", TEMP_PATH);
        } else {
            LOG.info("Using existing temporary data directory: {}", TEMP_PATH);
        }
    }

    /**
     * Reads a given path as an input stream and attaches metadata about the
     * path to the stream.
     *
     * @param path path to read
     * @return stream of path contents
     */
    PreprocessingInputStream readPath(final Path path) {
        final File file = path.toFile();
        try {
            InputStream origin = Files.newInputStream(path, StandardOpenOption.READ);
            InputStream buffer = new BufferedInputStream(origin, FILE_READ_BUFFER);
            DigestInputStream digester = new DigestInputStream(buffer, new FastMD5Digest());
            Instant lastModified = Instant.ofEpochMilli(file.lastModified());
            return new PreprocessingInputStream(digester, path, lastModified);
        } catch (IOException e) {
            String msg = "Error reading file from path";
            FileProcessingException fpe = new FileProcessingException(msg, e);
            fpe.setContextValue("path", path);

            throw fpe;
        }
    }

    /**
     * {@link OutputStream} that compresses data and stores embedded metadata
     * based on the specified path.
     *
     * @param path path to the source file that will be written to the stream
     * @return stream configured to write to a compressed temp file
     */
    PreprocessingOutputStream compressedTempFile(final Path path) {
        Path subPath = Paths.get(path + "." + ObjectCompressor.COMPRESSION_TYPE);
        Path tempPath = appendPaths(TEMP_PATH, subPath);
        Path parent = tempPath.getParent();

        if (tempPath.toFile().exists()) {
            Path changedPath = Paths.get(tempPath.getFileName() + "-" + UUID.randomUUID());
            LOG.warn("Avoiding overwrite of path [{}] by changing file name to [{}]",
                    tempPath, changedPath);
            tempPath = changedPath;
        } else if (!parent.toFile().exists()) {
            if (!parent.toFile().mkdirs() && !parent.toFile().exists()) {
                String msg = "Unable to create parent directory structure";
                FileProcessingException fpe = new FileProcessingException(msg);
                fpe.setContextValue("path", path);
                fpe.setContextValue("parentDir", parent);
                throw fpe;
            }
        }

        try {
            OutputStream fileOut = Files.newOutputStream(tempPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            OutputStream compressed = COMPRESSOR.compress(tempPath, fileOut);
            return new PreprocessingOutputStream(compressed, tempPath);
        } catch (IOException e) {
            String msg = "Unable to open path for write";
            FileProcessingException fpe = new FileProcessingException(msg, e);
            fpe.setContextValue("tempPath", tempPath);

            throw fpe;
        }
    }

    /**
     * Compresses a source file, writes it to a temp directory and returns an
     * object with metadata about the object.
     *
     * @param in source stream
     * @return object representing metadata and path of the temp file on the
     *         local filesystem
     */
    FileUpload buildFileToUpload(final PreprocessingInputStream in) {
        final Path path = in.getPath();
        final PreprocessingOutputStream out = compressedTempFile(path);

        try {
            IOUtils.copy(in, out);
        } catch (IOException e) {
            String msg = "Unable to copy and compress file contents";
            FileProcessingException fpe = new FileProcessingException(msg, e);
            fpe.setContextValue("path", path);

            throw fpe;
        } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }

        final Digest digest = in.getDigestInputStream().getDigest();
        final byte[] checksum = new byte[digest.getDigestSize()];
        digest.doFinal(checksum, 0);

        return new FileUpload(out.getTempPath(), in.getPath(), checksum,
                in.getLastModified(), in.getSize(), out.getSize());
    }

    /**
     * Adds an object to the queue for eventual upload. This method will preload
     * <code>queuePreloadSize</code> to the queue and then it will block until
     * an uploader becomes available before transferring another item to the
     * queue. This allows us to throttle the amount of compressed temp files
     * being created on the system.
     *
     * @param path path to object to add to upload queue
     */
    void addObjectToQueue(final Path path) {
        try {
            final File file = path.toFile();

            // Creates directory in temporary path
            if (file.isDirectory()) {
                appendPaths(TEMP_PATH, path).toFile().mkdirs();
                queue.put(new DirectoryUpload(path));
            } else {
                final PreprocessingInputStream in = readPath(path);

                if (LOG.isTraceEnabled() && !file.isDirectory()) {
                    LOG.trace("Started compressing [{}] [{} bytes]",
                            path, FileUtils.byteCountToDisplaySize(path.toFile().length()));
                }

                final FileUpload fileUpload = buildFileToUpload(in); // in is closed here

                if (LOG.isDebugEnabled() && !file.isDirectory()) {
                    LOG.debug("Finished compressing [{}] [{} -> {} {}]",
                            fileUpload.getSourcePath(),
                            FileUtils.byteCountToDisplaySize(fileUpload.getUncompressedSize()),
                            FileUtils.byteCountToDisplaySize(fileUpload.getCompressedSize()),
                            fileUpload.getCompressionPercentage());
                }

                if (preloadedCount.getAndIncrement() > queuePreloadSize) {
                    queue.transfer(fileUpload);
                } else {
                    queue.put(fileUpload);
                }
            }

            objectsProcessed.incrementAndGet();
        } catch (InterruptedException e) {
            LOG.warn("Object preloader thread interrupted");
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            LOG.error("Object couldn't be properly enqueued", e);
        }
    }

    /**
     * Creates a fork join task for each file that will need to be uploaded
     * to the remote filesystem. These tasks will be queued in the fork join
     * pool. When the tasks execute they will add an upload object to the
     * upload queue.
     *
     * @param root local working directory
     * @return value object containing details about the transfer
     */
    @SuppressWarnings("FutureReturnValueIgnored")
    TotalTransferDetails uploadDirectoryContents(final Path root) {
        final TotalTransferDetails transferDetails = new TotalTransferDetails();

        LocalFileUtils.directoryContentsStream(root)
                .forEach(p -> {
                    final File file = p.toFile().getAbsoluteFile();
                    final long size;

                    if (file.isDirectory()) {
                        size = 0L;
                    } else {
                        size = file.length();
                    }

                    transferDetails.numberOfBytes += size;
                    transferDetails.numberOfObjects++;

                    executor.execute(() -> addObjectToQueue(p));
                });

        return transferDetails;
    }

    /**
     * @return reference to upload queue
     */
    TransferQueue<ObjectUpload> getQueue() {
        return queue;
    }

    /**
     * @return total number of objects added to the queue
     */
    AtomicLong getObjectsProcessed() {
        return objectsProcessed;
    }

    /**
     * Combines two path objects such that the first object contains the
     * second object. For example, <pre>/tmp</pre> and <pre>/opt/foo</pre>
     * would combine to make <pre>/tmp/opt/foo</pre>.
     *
     * @param parent directory to become the parent
     * @param subPath directory or file to nest within the parent
     * @return a path combining the parent and the subPath
     */
    static Path appendPaths(final Path parent, final Path subPath) {
        final Path withoutParentSubPath = subPath.subpath(0, subPath.getNameCount());
        return parent.resolve(withoutParentSubPath);
    }
}
