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
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Set;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

public class MantaTransferClient implements TransferClient {
    private static final Logger LOG = LoggerFactory.getLogger(MantaTransferClient.class);

    private static final int DIRECTORY_CACHE_SIZE = 4096;

    private Set<String> dirCache = Collections.newSetFromMap(
            new LRUMap<>(DIRECTORY_CACHE_SIZE));

    private MantaClient client;

    public MantaTransferClient(final MantaClient client, final String mantaRoot) {
        this.client = client;

        populateDirectoryCache(mantaRoot);
    }

    private void populateDirectoryCache(final String mantaRoot) {
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
            client.putDirectory(path, true);
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
            throws MantaClientHttpResponseException{
        final String dir = FilenameUtils.getFullPath(path);
        final File file = upload.getTempPath().toFile();
        final String base64Checksum = Base64.encodeBase64String(upload.getChecksum());

        try {
            if (!dirCache.contains(dir)) {
                LOG.debug("Parent directory is already not in cache [{}] for file", dir, path);
                client.putDirectory(dir, true);
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
            metadata.put("m-uncompressed-size", Long.toString(upload.getUncompressedSize()));
            metadata.put("m-original-path", upload.getSourcePath().toString());
            metadata.put("m-original-md5", base64Checksum);

            LOG.debug("Uploading file [{}] --> [{}]", upload.getSourcePath(), path);
            return client.put(path, file, headers, metadata);
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

    private MantaObjectResponse checkForRemoteFile(final String path) throws IOException {
        try {
            return client.head(path);
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            }

            throw e;
        }
    }

    @Override
    public int getMaximumConcurrentUploads() {
        return this.client.getContext().getMaximumConnections();
    }

    @Override
    public void close() {
        client.close();
    }
}
