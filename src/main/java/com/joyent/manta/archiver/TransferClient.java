/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

public interface TransferClient extends AutoCloseable {
    int getMaximumConcurrentUploads();
    void mkdirp(String path, DirectoryUpload upload);
    void put(String path, FileUpload upload);
    @Override
    void close();
}
