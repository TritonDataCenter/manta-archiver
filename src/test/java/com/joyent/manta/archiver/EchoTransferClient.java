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
 * A {@link TransferClient} implementation used for testing that outputs
 * files transferred to STDOUT.
 */
class EchoTransferClient implements TransferClient {
    @Override
    public void mkdirp(final String path, final DirectoryUpload upload) {
        System.out.println("mkdirp: " + upload.getSourcePath() + " --> " + path);
    }

    @Override
    public void put(final String path, final FileUpload upload) {
        System.out.println("put:    " + upload.getSourcePath() + " --> " + path);
    }

    @Override
    public int getMaximumConcurrentUploads() {
        return 12;
    }

    @Override
    public String convertLocalPathToRemotePath(final ObjectUpload upload, final Path localRoot) {
        return "/remote/" + localRoot;
    }

    @Override
    public void close() {
        // Do nothing
    }
}
