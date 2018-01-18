/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.twmacinta.util.MD5;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link Runnable} implementation that handles file downloads.
 */
class ObjectDownloadRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectDownloadRunnable.class);
    private static final String OUTPUT_FORMAT = "[%s] %s --> %s" + System.lineSeparator();

    private Path path;
    private TransferClient client;
    private FileDownload fileDownload;
    private AtomicBoolean verificationSuccess;
    private AtomicLong totalObjectsProcessed;

    /**
     * Creates a new instance.
     *
     * @param path path to local file to write remote file to
     * @param client reference to transfer client
     * @param fileDownload remote object
     * @param verificationSuccess atomic boolean flag indicated everything succeeded
     * @param totalObjectsProcessed atomic long counting total files downlaoded
     */
    ObjectDownloadRunnable(final Path path,
                           final TransferClient client,
                           final FileDownload fileDownload,
                           final AtomicBoolean verificationSuccess,
                           final AtomicLong totalObjectsProcessed) {
        this.path = path;
        this.client = client;
        this.fileDownload = fileDownload;
        this.verificationSuccess = verificationSuccess;
        this.totalObjectsProcessed = totalObjectsProcessed;
    }

    @Override
    public void run() {
        try {
            if (localFileIsTheSameAsRemote(path)) {
                String centered = StringUtils.center("EXISTS", VerificationResult.MAX_STRING_SIZE);
                System.err.printf(OUTPUT_FORMAT, centered, fileDownload.getRemotePath(), path);
                return;
            }

            final VerificationResult result = client.download(
                    fileDownload.getRemotePath(), Optional.of(path));

            fileDownload.setLink(VerificationResult.LINK_OK.equals(result));

            if (verificationSuccess.get() && !result.isOk()) {
                verificationSuccess.set(false);
            }

            String centered = StringUtils.center(result.toString(), VerificationResult.MAX_STRING_SIZE);
            System.err.printf(OUTPUT_FORMAT, centered, fileDownload.getRemotePath(), path);
        } catch (RuntimeException e) {
            if (e instanceof TransferClientException) {
                ((TransferClientException)e).setContextValue("fileDownload", fileDownload);
            }

            LOG.error("Error downloading object", e);
        } finally {
            totalObjectsProcessed.incrementAndGet();
        }
    }

    private boolean localFileIsTheSameAsRemote(final Path localPath) {
        if (!Files.exists(localPath, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }

        if (Files.isSymbolicLink(localPath)) {
            try {
                final Path resolvedLink = Files.readSymbolicLink(localPath);
                return client.verifyLink(fileDownload.getRemotePath(),
                        resolvedLink).equals(VerificationResult.LINK_OK);
            } catch (IOException e) {
                String msg = String.format("Unable to process symbolic link: %s",
                        localPath);
                LOG.error(msg, e);
                return false;
            } catch (UncheckedIOException e) {
                final IOException cause = e.getCause();

                if (cause instanceof NoSuchFileException) {
                    return false;
                } else {
                    String msg = String.format("Unable to process symbolic link: %s",
                            localPath);
                    LOG.error(msg, e);
                    return false;
                }
            }
        }

        try {
            final long size = Files.size(localPath);
            final byte[] checksum = MD5.getHash(localPath.toFile());

            // Don't download if we already have the file
            if (client.verifyFile(fileDownload.getRemotePath(),
                    size, checksum).equals(VerificationResult.OK)) {
                return true;
            }
        } catch (RuntimeException | IOException e) {
            String msg = String.format("Unable to checksum file: %s", localPath);
            LOG.error(msg, e);
        }

        return false;
    }
}
