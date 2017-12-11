/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Interface describing the functionality needed to upload files to a remote
 * data store.
 */
public interface TransferClient extends AutoCloseable {
    /**
     * @return the maximum number of concurrent uploads supported by the client
     */
    int getMaximumConcurrentUploads();

    /**
     * Finds all files in the remote file system recursively within the
     * implied base path.
     *
     * @return stream of remote objects
     */
    Stream<String> find();

    /**
     * Creates a directory and all required subdirectories.
     *
     * @param path path to create
     * @param upload directory object in which the path is based on
     */
    void mkdirp(String path, DirectoryUpload upload);

    /**
     * Uploads a file to the specified path.
     *
     * @param path path to upload to
     * @param upload upload object to read file from
     */
    void put(String path, FileUpload upload);

    /**
     * Verifies that the specified directory exists on the remote file system.
     * @param remotePath path to remote directory
     * @return enum representing verification status
     */
    VerificationResult verifyDirectory(String remotePath);

    /**
     * Verifies that the specified file exists on the remote file system
     * and the contents match the file size and specified checksum.
     *
     * @param remotePath path to remote file
     * @param size expected file size
     * @param checksum expected checksum
     * @return enum representing verification status
     */
    VerificationResult verifyFile(String remotePath, long size, byte[] checksum);

    /**
     * Verifies that the specified file exists on the remote file system
     * and that the contents match the checksum and file size specified in
     * the object's metadata.
     *
     * @param remotePath path to remote file
     * @return enum representing verification status
     */
    VerificationResult verifyRemoteFile(String remotePath);

    /**
     * Converts a local path to a remote filesystem path.
     *
     * @param sourcePath source path of the object in which the path will be converted
     * @param localRoot local filesystem working directory path
     *
     * @return converted path
     */
    String convertLocalPathToRemotePath(Path sourcePath, Path localRoot);

    /**
     * @return the base remote path in which files or directories are uploaded to
     */
    String getRemotePath();

    @Override
    void close();
}
