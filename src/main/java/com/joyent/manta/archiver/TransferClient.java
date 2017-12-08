/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import java.nio.file.Path;

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
     * Converts a local path to a remote filesystem path.
     *
     * @param upload object in which the path will be converted
     * @param localRoot local filesystem working directory path
     *
     * @return converted path
     */
    String convertLocalPathToRemotePath(ObjectUpload upload,
                                        Path localRoot);

    /**
     * @return the base remote path in which files or directories are uploaded to
     */
    String getRemotePath();

    @Override
    void close();
}
