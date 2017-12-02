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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.io.File.separator;

/**
 * Class in charge of compressing files and doing checksums on files that will
 * be copied to Manta.
 */
public class ObjectUploadQueueLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectUploadQueueLoader.class);

    private static final int FILE_READ_BUFFER = 16_384;

    private static final ObjectCompressor COMPRESSOR = new ObjectCompressor();
    private static final Path TEMP_PATH = Paths.get(System.getProperty("java.io.tmpdir")
            + separator + "manta-archiver");

    private final ForkJoinPool executor;
    private final TransferQueue<ObjectUpload> queue;
    private final int queuePreloadSize;
    private final AtomicInteger preloadedCount = new AtomicInteger();

    static {
        // Queue up deletion of temp files when process exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Deleting temp files");

            try {
                FileUtils.forceDelete(TEMP_PATH.toFile());
            } catch (IOException e) {
                LOG.info("Unable to delete temp files from path: " + TEMP_PATH, e);
            }
        }));
    }

    public ObjectUploadQueueLoader(final ForkJoinPool executor, final int queuePreloadSize) {
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

    Stream<Path> directoryContentsStream(final Path root) {
        try {
            return Files.walk(root);
        } catch (IOException e) {
            String msg = "Unable to recursively traverse path";
            FileProcessingException fpe = new FileProcessingException(msg, e);
            fpe.setContextValue("rootPath", root);

            throw fpe;
        }
    }

    PreprocessingOutputStream compressedTempFile(final Path path) {
        Path subPath = Paths.get(path + "." + ObjectCompressor.COMPRESSION_TYPE);
        Path tempPath = appendPaths(TEMP_PATH, subPath);

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

    void addObjectToQueue(final Path path) {
        try {
            // Creates directory in temporary path
            if (path.toFile().isDirectory()) {
                appendPaths(TEMP_PATH, path).toFile().mkdirs();
                queue.transfer(new DirectoryUpload(path));
                return;
            }

            PreprocessingInputStream in = readPath(path);
            FileUpload fileUpload = buildFileToUpload(in); // in is closed here

            LOG.debug("Finished compressing [{}]", fileUpload.getSourcePath());

            if (preloadedCount.getAndIncrement() > queuePreloadSize) {
                queue.transfer(fileUpload);
            } else {
                queue.put(fileUpload);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    long processDirectoryContents(final Path root) {
        return directoryContentsStream(root)
                .map(p -> executor.submit(() -> addObjectToQueue(p)))
                .count();
    }

    public TransferQueue<ObjectUpload> getQueue() {
        return queue;
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
