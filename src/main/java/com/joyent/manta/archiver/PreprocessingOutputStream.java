/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * {@link java.io.OutputStream} implementation that preserves access to fields so that
 * they can be used for debugging later.
 */
class PreprocessingOutputStream extends FilterOutputStream {
    private final Path tempPath;

    /**
     * Creates new instance.
     *
     * @param out OutputStream to wrap
     * @param tempPath path to temporary file in which the wrapped stream is writing to
     */
    PreprocessingOutputStream(final OutputStream out, final Path tempPath) {
        super(out);
        this.tempPath = tempPath;
    }

    Path getTempPath() {
        return tempPath;
    }

    long getSize() {
        return tempPath.toFile().length();
    }
}
