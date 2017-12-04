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
 * Interface representing an object to upload to Manta.
 */
interface ObjectUpload {
    /**
     * @return path to the original file
     */
    Path getSourcePath();

    /**
     * @return true if the object is a directory
     */
    boolean isDirectory();
}
