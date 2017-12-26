/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.MantaUtils;
import com.twmacinta.util.FastMD5Digest;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.io.DigestInputStream;
import org.bouncycastle.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Objects.requireNonNull;

/**
 * {@link TransferClient} implementation that allows for the transfer of files
 * to and from Manta.
 */
class MantaTransferClient implements TransferClient {
    private static final Logger LOG = LoggerFactory.getLogger(MantaTransferClient.class);

    private static final int DIRECTORY_CACHE_SIZE = 16384;
    private static final int CACHE_ACCESS_EXPIRATION_HOURS = 4;
    private static final String UNCOMPRESSED_SIZE_HEADER = "m-uncompressed-size";
    private static final String ORIGINAL_PATH_HEADER = "m-original-path";
    private static final String ORIGINAL_MD5_HEADER = "m-original-md5";

    /**
     * Function that converts a {@link MantaObject} to a {@link FileDownload}.
     */
    private static final Function<MantaObject, FileDownload> OBJ_TO_DOWNLOAD_FUNCTION = o -> {
        final String mantaPath;

        // If we have been given a file with a trailing separator character,
        // we fix it here, so that it doesn't impact downstream processes.
        if (!o.isDirectory() && o.getPath().endsWith(MantaClient.SEPARATOR)) {
            mantaPath = StringUtils.removeEnd(o.getPath(), MantaClient.SEPARATOR);
        } else {
            mantaPath = o.getPath();
        }

        final Long lastModified;

        if (o.getLastModifiedTime() != null) {
            lastModified = o.getLastModifiedTime().getTime();
        } else {
            lastModified = null;
        }

        return new FileDownload(o.getContentLength(),
                lastModified,
                mantaPath,
                o.isDirectory());
    };

    private Cache<String, Boolean> dirCache = CacheBuilder.newBuilder()
            .maximumSize(DIRECTORY_CACHE_SIZE)
            .softValues()
            .expireAfterAccess(CACHE_ACCESS_EXPIRATION_HOURS, TimeUnit.HOURS)
            .build();

    private final AtomicReference<MantaClient> clientRef;

    /**
     * Path to the remote working directory.
     */
    private final String mantaRoot;

    /**
     * When the remote path is pointing to a single file, this variable contains
     * that single filename.
     */
    private final String singleFile;

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

        final String normalized = normalize(mantaRoot);

        // Early exit the constructor if there is no Manta client specified so
        // that we can skip all associated checks and normalization logic.
        if (clientSupplier == null) {
            this.mantaRoot = normalized;
            this.singleFile = null;
        } else {
            try {
                MantaObjectResponse head = clientRef.get().head(normalized);

                if (head.isDirectory()) {
                    this.mantaRoot = normalized;
                    this.singleFile = null;
                } else {
                    String noSeparator = StringUtils.removeEnd(normalized, MantaClient.SEPARATOR);
                    String parent = FilenameUtils.getFullPath(noSeparator);
                    this.mantaRoot = parent;
                    this.singleFile = FilenameUtils.getName(noSeparator);
                }
            } catch (IOException e) {
                if (e instanceof MantaClientHttpResponseException) {
                    MantaClientHttpResponseException mchre = (MantaClientHttpResponseException) e;

                    if (mchre.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                        String msg = String.format("Remote path does not exist: %s", normalized);
                        throw new TransferClientException(msg);
                    }
                }

                String msg = "Error accessing remote Manta path";
                TransferClientException tce = new TransferClientException(msg, e);
                tce.setContextValue("mantaPath", normalized);
                throw tce;
            }

            populateDirectoryCache();
        }
    }

    private void populateDirectoryCache() {
        LOG.debug("Populating directory cache");
        if (mantaRoot.endsWith(MantaClient.SEPARATOR)) {
            dirCache.put(mantaRoot, true);
        } else {
            dirCache.put(mantaRoot + MantaClient.SEPARATOR, true);
        }
    }

    @Override
    public Stream<FileDownload> find() {
        if (singleFile != null) {
            try {
                String path = mantaRoot + singleFile;
                MantaObjectResponse head = clientRef.get().head(path);
                return Stream.of(OBJ_TO_DOWNLOAD_FUNCTION.apply(head));
            } catch (IOException e) {
                String msg = "Unable to find remote object";
                TransferClientException tce = new TransferClientException(msg, e);
                tce.setContextValue("mantaPath", mantaRoot);
                throw tce;
            }
        }

        return clientRef.get().find(mantaRoot).map(OBJ_TO_DOWNLOAD_FUNCTION);
    }

    @Override
    public void mkdirp(final String path, final DirectoryUpload upload) {
        if (BooleanUtils.toBoolean(dirCache.getIfPresent(path))) {
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

        dirCache.put(path, true);
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
            if (!BooleanUtils.toBoolean(dirCache.getIfPresent(dir))) {
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

            String sourcePath = MantaUtils.formatPath(upload.getSourcePath().toString());
            metadata.put(ORIGINAL_PATH_HEADER, sourcePath);
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
                LOG.info("No {} header for {}", UNCOMPRESSED_SIZE_HEADER, remotePath);
                return VerificationResult.MISSING_HEADERS;
            }

            final long actualSize = Long.parseLong(originalSize);

            if (size != actualSize) {
                LOG.info("Remote file different size [{}] than expected [{}]",
                        actualSize, size);
                return VerificationResult.WRONG_SIZE;
            }

            final String md5Base64 = response.getHeaderAsString(ORIGINAL_MD5_HEADER);

            if (md5Base64 == null) {
                LOG.info("No {} header for {}", ORIGINAL_MD5_HEADER, remotePath);
                return VerificationResult.MISSING_HEADERS;
            }

            final byte[] originalMd5 = java.util.Base64.getDecoder().decode(md5Base64);

            if (!Arrays.areEqual(checksum, originalMd5)) {
                LOG.info("Checksums do not match for object {}", remotePath);
                return VerificationResult.CHECKSUM_MISMATCH;
            }

            return VerificationResult.OK;
        } catch (IOException e) {
            if (e instanceof MantaClientHttpResponseException) {
                MantaClientHttpResponseException mchre = (MantaClientHttpResponseException)e;

                if (mchre.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    LOG.info("Couldn't find remote path {}");
                    return VerificationResult.NOT_FOUND;
                }
            }

            String msg = "Unable to verify file";
            TransferClientException tce = new TransferClientException(msg, e);
            tce.setContextValue("mantaPath", remotePath);
            throw tce;
        }
    }

    @Override
    public VerificationResult download(final String remotePath, final OutputStream out,
                                       final Optional<File> file) {
        final byte[] expectedChecksum;
        final byte[] actualChecksum;
        final long expectedSize;
        final long lastModified;

        try (MantaObjectInputStream in = clientRef.get().getAsInputStream(remotePath);
             XZCompressorInputStream xzIn = new XZCompressorInputStream(in);
             CountingInputStream cIn = new CountingInputStream(xzIn);
             DigestInputStream dIn = new DigestInputStream(cIn, new FastMD5Digest())) {

            final String md5Base64 = in.getHeaderAsString(ORIGINAL_MD5_HEADER);

            if (md5Base64 == null) {
                LOG.info("No {} header for {}", ORIGINAL_MD5_HEADER, remotePath);
                return VerificationResult.MISSING_HEADERS;
            }

            expectedChecksum = java.util.Base64.getDecoder().decode(md5Base64);

            final String originalSize = in.getHeaderAsString(UNCOMPRESSED_SIZE_HEADER);

            if (originalSize == null) {
                LOG.info("No {} header for {}", UNCOMPRESSED_SIZE_HEADER, remotePath);
                return VerificationResult.MISSING_HEADERS;
            }

            expectedSize = Long.parseLong(originalSize);

            LOG.debug("Downloading [{}]", remotePath);
            IOUtils.copyLarge(dIn, out, 0, expectedSize);

            final Digest digest = dIn.getDigest();
            actualChecksum = new byte[digest.getDigestSize()];
            digest.doFinal(actualChecksum, 0);

            if (dIn.read() != -1) {
                LOG.info("{} was larger than the expected size of {}");
                return VerificationResult.WRONG_SIZE;
            }

            if (in.getLastModifiedTime() != null) {
                lastModified = in.getLastModifiedTime().getTime();
            } else {
                lastModified = Instant.now().toEpochMilli();
            }
        } catch (RuntimeException | IOException e) {
            if (e instanceof MantaClientHttpResponseException) {
                MantaClientHttpResponseException mchre = (MantaClientHttpResponseException)e;

                if (mchre.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    LOG.info("Couldn't find {}", remotePath);
                    return VerificationResult.NOT_FOUND;
                }
            }

            String msg = "Unable to download remote file";
            TransferClientException tce = new TransferClientException(msg, e);
            tce.setContextValue("mantaPath", remotePath);
            throw tce;
        }

        if (!Arrays.areEqual(expectedChecksum, actualChecksum)) {
            LOG.info("Checksums do not match for {}", remotePath);
            return VerificationResult.CHECKSUM_MISMATCH;
        }

        if (file.isPresent()) {
            if (!file.get().setLastModified(lastModified)) {
                LOG.warn("Unable to write last modified date for file: {}", file);
            }

            final long fileSize = file.get().length();
            if (fileSize != expectedSize) {
                LOG.info("Incorrect number of bytes written to file [{}]. Actual: {} Expected: {}",
                        file, fileSize, expectedSize);
                return VerificationResult.WRONG_SIZE;
            }
        }

        return VerificationResult.OK;
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
    public int getMaximumConcurrentConnections() {
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
    public Path convertRemotePathToLocalPath(final String remotePath, final Path localRoot) {
        final String relativePath = StringUtils.removeFirst(remotePath, mantaRoot);
        final String extension = "." + ObjectCompressor.COMPRESSION_TYPE;
        final String withoutCompressionSuffix = StringUtils.removeEnd(relativePath, extension);

        return Paths.get(localRoot.toString(), withoutCompressionSuffix);
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
