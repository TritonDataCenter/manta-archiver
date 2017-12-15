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
 * Class that represents a directory to be added to Manta.
 */
class DirectoryUpload implements ObjectUpload {
    private final Path sourcePath;

    /**
     * Creates new instance of a directory upload object.
     *
     * @param sourcePath path to original directory
     */
    DirectoryUpload(final Path sourcePath) {
        this.sourcePath = sourcePath;
    }

    @Override
    public Path getSourcePath() {
        return sourcePath;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }
}
