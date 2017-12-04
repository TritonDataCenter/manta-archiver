/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.bouncycastle.crypto.io.DigestInputStream;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;

/**
 * {@link InputStream} implementation that preserves access to fields so that
 * they can be used for debugging later.
 */
public class PreprocessingInputStream extends FilterInputStream {
    private final Path path;
    private final DigestInputStream digestInputStream;
    private final Instant lastModified;

    /**
     * Creates a new instance based on a checksum stream, the local file path
     * and a last modified data.
     *
     * @param in checksum stream
     * @param path path to file
     * @param lastModified last time the file was modified
     */
    PreprocessingInputStream(final DigestInputStream in, final Path path,
                             final Instant lastModified) {
        super(in);
        this.digestInputStream = in;
        this.path = path;
        this.lastModified = lastModified;
    }

    DigestInputStream getDigestInputStream() {
        return digestInputStream;
    }

    Path getPath() {
        return path;
    }

    Instant getLastModified() {
        return lastModified;
    }

    long getSize() {
        return path.toFile().length();
    }
}
