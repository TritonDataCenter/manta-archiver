/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpStatus;
import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Objects.requireNonNull;

/**
 * {@link TransferClient} implementation that allows for the transfer of files
 * to and from Manta.
 */
class MantaTransferClient implements TransferClient {
    private static final Logger LOG = LoggerFactory.getLogger(MantaTransferClient.class);

    private static final int DIRECTORY_CACHE_SIZE = 4096;
    private static final String UNCOMPRESSED_SIZE_HEADER = "m-uncompressed-size";
    private static final String ORIGINAL_PATH_HEADER = "m-original-path";
    private static final String ORIGINAL_MD5_HEADER = "m-original-md5";

    private Set<String> dirCache = Collections.newSetFromMap(
            new LRUMap<>(DIRECTORY_CACHE_SIZE));

    private final AtomicReference<MantaClient> clientRef;
    private final String mantaRoot;

    /**
     * Creates a new instance based on the specified Manta client and the
     * remote working directory.
     *
     * @param clientSupplier Manta client supplier that provides configured MantaClient instances
     * @param mantaRoot remote working directory
     */
    MantaTransferClient(final Supplier<MantaClient> clientSupplier, final String mantaRoot) {
        // A null supplier is only ever valid when testing
        if (clientSupplier == null) {
            this.clientRef = null;
        } else {
            this.clientRef = new AtomicReference<>(clientSupplier.get());
        }

        this.mantaRoot = normalize(mantaRoot);

        populateDirectoryCache();
    }

    private void populateDirectoryCache() {
        LOG.debug("Populating directory cache");
        if (mantaRoot.endsWith(MantaClient.SEPARATOR)) {
            dirCache.add(mantaRoot);
        } else {
            dirCache.add(mantaRoot + MantaClient.SEPARATOR);
        }
    }

    @Override
    public void mkdirp(final String path, final DirectoryUpload upload) {
        if (dirCache.contains(path)) {
            LOG.debug("Directory is already in cache [{}]", path);
            return;
        }

        try {
            clientRef.get().putDirectory(path, true);
        } catch (IOException e) {
            String msg = "Unable to create directory";
            TransferClientException tce = new TransferClientException(msg, e);
            tce.setContextValue("upload", upload);
            tce.setContextValue("mantaPath", path);
            throw tce;
        }

        dirCache.add(path);
    }

    @Override
    public void put(final String path, final FileUpload upload) {
        try {
            put(path, upload, false);
        } catch (MantaClientHttpResponseException e) {
            MantaClientHttpResponseException thrownException;

            // Precondition failed means we tried to overwrite and existing file
            if (e.getStatusCode() == HttpStatus.SC_PRECONDITION_FAILED) {
                try {
                    put(path, upload, true);
                    return;
                } catch (MantaClientHttpResponseException innerE) {
                    thrownException = innerE;
                }
            } else {
                thrownException = e;
            }

            String msg = "Unable to upload file";
            TransferClientException tce = new TransferClientException(msg, thrownException);
            tce.setContextValue("upload", upload);
            tce.setContextValue("mantaPath", path);
            throw tce;
        }
    }

    private MantaObjectResponse put(final String path, final FileUpload upload, final boolean overwrite)
            throws MantaClientHttpResponseException {
        final String dir = FilenameUtils.getFullPath(path);
        final File file = upload.getTempPath().toFile();
        final String base64Checksum = Base64.encodeBase64String(upload.getChecksum());

        if (!file.exists()) {
            String msg = String.format("Something went wrong. The file [%s] is "
                    + "no longer available for upload. Please make sure that "
                    + "there is no process deleting temp files. Upload details: %s%s",
                    file.getAbsolutePath(),
                    upload, System.lineSeparator());
            System.err.println(msg);
            System.exit(1);
        }

        try {
            if (!dirCache.contains(dir)) {
                LOG.debug("Parent directory is already not in cache [{}] for file", dir, path);
                clientRef.get().putDirectory(dir, true);
            }

            final String httpLastModified = RFC_1123_DATE_TIME.format(
                    upload.getLastModified().atZone(ZoneOffset.UTC));
            final MantaHttpHeaders headers = new MantaHttpHeaders()
                    .setLastModified(httpLastModified);

            /* If we are in overwrite mode, then we attempt to see if the remote
             * file already exists. If it doesn't then we proceed. If it does,
             * then we check to see if the file size and checksums match. If they
             * do match, then we don't need to overwrite the file because the
             * file already exists. */
            if (overwrite) {
                final MantaObjectResponse head = checkForRemoteFile(path);

                if (head != null && base64Checksum.equals(head.getHeaderAsString("m-original-md5"))) {
                    LOG.debug("Local [{}] and remote file [{}] match - not uploading",
                            upload.getSourcePath(), path);
                    return head;
                }
            } else {
                // Only PUT if the remote file doesn't exist
                headers.setIfMatch("\"\"");
            }

            final MantaMetadata metadata = new MantaMetadata();
            metadata.put(UNCOMPRESSED_SIZE_HEADER, Long.toString(upload.getUncompressedSize()));
            metadata.put(ORIGINAL_PATH_HEADER, upload.getSourcePath().toString());
            metadata.put(ORIGINAL_MD5_HEADER, base64Checksum);

            LOG.debug("Uploading file [{}] --> [{}]", upload.getSourcePath(), path);
            return clientRef.get().put(path, file, headers, metadata);
        } catch (IOException e) {
            if (e instanceof MantaClientHttpResponseException) {
                throw (MantaClientHttpResponseException)e;
            }

            String msg = "Unable to upload file";
            TransferClientException tce = new TransferClientException(msg, e);
            tce.setContextValue("upload", upload);
            tce.setContextValue("mantaPath", path);
            throw tce;
        }
    }

    @Override
    public VerificationResult verifyDirectory(final String remotePath) {
        try {
            MantaObjectResponse response = clientRef.get().head(remotePath);

            if (!response.isDirectory()) {
                return VerificationResult.NOT_DIRECTORY;
            }

            return VerificationResult.OK;
        } catch (IOException e) {
            if (e instanceof MantaClientHttpResponseException) {
                MantaClientHttpResponseException mchre = (MantaClientHttpResponseException)e;

                if (mchre.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    return VerificationResult.NOT_FOUND;
                }
            }

            String msg = "Unable to verify directory";
            TransferClientException tce = new TransferClientException(msg, e);
            tce.setContextValue("mantaPath", remotePath);
            throw tce;
        }
    }

    @Override
    public VerificationResult verifyFile(final String remotePath, final long size,
            final byte[] checksum) {
        try {
            MantaObjectResponse response = clientRef.get().head(remotePath);

            if (response.isDirectory()) {
                return VerificationResult.NOT_FILE;
            }

            final String originalSize = response.getHeaderAsString(UNCOMPRESSED_SIZE_HEADER);

            if (originalSize == null) {
                return VerificationResult.MISSING_HEADERS;
            }

            if (size != Long.parseLong(originalSize)) {
                return VerificationResult.WRONG_SIZE;
            }

            final String md5Base64 = response.getHeaderAsString(ORIGINAL_MD5_HEADER);

            if (md5Base64 == null) {
                return VerificationResult.MISSING_HEADERS;
            }

            final byte[] originalMd5 = java.util.Base64.getDecoder().decode(md5Base64);

            if (!Arrays.areEqual(checksum, originalMd5)) {
                return VerificationResult.CHECKSUM_MISMATCH;
            }

            return VerificationResult.OK;
        } catch (IOException e) {
            if (e instanceof MantaClientHttpResponseException) {
                MantaClientHttpResponseException mchre = (MantaClientHttpResponseException)e;

                if (mchre.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    return VerificationResult.NOT_FOUND;
                }
            }

            String msg = "Unable to verify file";
            TransferClientException tce = new TransferClientException(msg, e);
            tce.setContextValue("mantaPath", remotePath);
            throw tce;
        }
    }

    private MantaObjectResponse checkForRemoteFile(final String path) throws IOException {
        try {
            return clientRef.get().head(path);
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            }

            throw e;
        }
    }

    @Override
    public int getMaximumConcurrentUploads() {
        return this.clientRef.get().getContext().getMaximumConnections();
    }

    @Override
    public String convertLocalPathToRemotePath(final Path sourcePath,
                                               final Path localRoot) {
        Path subPath = localRoot.relativize(sourcePath);

        StringBuilder builder = new StringBuilder(mantaRoot);

        Iterator<Path> itr = subPath.iterator();

        String filename = "";

        while (itr.hasNext()) {
            filename = itr.next().getFileName().toString();

            if (filename.isEmpty()) {
                continue;
            }

            // Add a separator if it didn't exist
            if (!filename.startsWith(MantaClient.SEPARATOR)) {
                builder.append(MantaClient.SEPARATOR);
            }

            builder.append(filename);
        }

        final boolean isDirectory = sourcePath.toFile().isDirectory();

        if (isDirectory && !filename.isEmpty() && !filename.endsWith(MantaClient.SEPARATOR)) {
            builder.append(MantaClient.SEPARATOR);
        } else if (!isDirectory) {
            builder.append(".").append(ObjectCompressor.COMPRESSION_TYPE);
        }

        return FilenameUtils.normalize(builder.toString(), true);
    }

    @Override
    public String getRemotePath() {
        return this.mantaRoot;
    }

    @Override
    public void close() {
        clientRef.get().close();
    }

    /**
     * Normalizes the directory structure of the remote path provided.
     *
     * @param path path to normalize
     * @return normalized path
     */
    private String normalize(final String path) {
        final MantaClient client;

        if (clientRef != null) {
            client = this.clientRef.get();
        } else {
            client = null;
        }

        final String mantaHomeDir;

        if (client != null) {
            mantaHomeDir = client.getContext().getMantaHomeDirectory();
        } else {
            mantaHomeDir = "";
        }

        final String substitute = substituteHomeDirectory(path, mantaHomeDir);
        final String normalized = FilenameUtils.normalize(substitute, true);

        if (normalized.endsWith(MantaClient.SEPARATOR)) {
            return normalized;
        } else {
            return normalized + MantaClient.SEPARATOR;
        }
    }

    /**
     * Substitutes the actual home directory for ~~ in a given path.
     *
     * @param path path to substitute
     * @param homeDir home directory to replace ~~ with
     * @return interpolate path
     */
    static String substituteHomeDirectory(final String path, final String homeDir) {
        requireNonNull(path, "Path is null");
        requireNonNull(homeDir, "Home directory is null");

        if (path.startsWith("~~")) {
            return MantaUtils.formatPath(homeDir + path.substring(2));
        } else {
            return MantaUtils.formatPath(path);
        }
    }
}
