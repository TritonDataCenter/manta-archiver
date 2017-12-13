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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link Callable} implementation that handles file downloads.
 */
class ObjectDownloadCallable implements Callable<Void> {
    private static final String OUTPUT_FORMAT = "[%s] %s --> %s" + System.lineSeparator();

    private Path path;
    private TransferClient client;
    private String remoteObject;
    private AtomicBoolean verificationSuccess;
    private AtomicLong totalObjectsProcessed;

    /**
     * Creates a new instance.
     *
     * @param path path to local file to write remote file to
     * @param client reference to transfer client
     * @param remoteObject path to remote object
     * @param verificationSuccess atomic boolean flag indicated everything succeeded
     * @param totalObjectsProcessed atomic long counting total files downlaoded
     */
    ObjectDownloadCallable(final Path path,
                           final TransferClient client,
                           final String remoteObject,
                           final AtomicBoolean verificationSuccess,
                           final AtomicLong totalObjectsProcessed) {
        this.path = path;
        this.client = client;
        this.remoteObject = remoteObject;
        this.verificationSuccess = verificationSuccess;
        this.totalObjectsProcessed = totalObjectsProcessed;
    }

    @Override
    public Void call() throws Exception {
        final File file = path.toFile();

        if (file.exists()) {
            final byte[] checksum;
            final long size = file.length();

            try {
                 checksum = MD5.getHash(file);
            } catch (IOException e) {
                String msg = "Unable to checksum file";
                TransferClientException tce = new TransferClientException(msg, e);
                tce.setContextValue("localFile", file);
                throw tce;
            }

            // Don't download if we already have the file
            if (client.verifyFile(remoteObject, size, checksum).equals(VerificationResult.OK)) {
                String centered = StringUtils.center("EXISTS", VerificationResult.MAX_STRING_SIZE);
                System.err.printf(OUTPUT_FORMAT, centered, remoteObject, path);
                return null;
            }
        }

        final OpenOption[] options = new OpenOption[] {
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
        };

        try (OutputStream out = Files.newOutputStream(path, options)) {
            final VerificationResult result = client.download(remoteObject, out,
                    Optional.of(file));

            if (verificationSuccess.get() && !result.equals(VerificationResult.OK)) {
                verificationSuccess.set(false);
            }

            totalObjectsProcessed.incrementAndGet();
            String centered = StringUtils.center(result.toString(), VerificationResult.MAX_STRING_SIZE);
            System.err.printf(OUTPUT_FORMAT, centered, remoteObject, path);
        }

        return null;
    }
}
