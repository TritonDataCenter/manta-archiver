/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.math3.util.Precision;
import org.bouncycastle.util.encoders.Hex;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Class that provides properties about a file that has been compressed and
 * is pending upload to Manta.
 */
public class FileUpload implements ObjectUpload {
    private final Path sourcePath;
    private final Path tempPath;
    private final byte[] checksum;
    private final Instant lastModified;
    private final long uncompressedSize;
    private final long compressedSize;

    public FileUpload(final Path tempPath, final Path sourcePath,
                      final byte[] checksum, final Instant lastModified,
                      final long uncompressedSize, final long compressedSize) {
        this.sourcePath = sourcePath;
        this.tempPath = tempPath;
        this.checksum = checksum;
        this.lastModified = lastModified;
        this.uncompressedSize = uncompressedSize;
        this.compressedSize = compressedSize;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public Path getSourcePath() {
        return sourcePath;
    }

    public Path getTempPath() {
        return tempPath;
    }

    public byte[] getChecksum() {
        return checksum;
    }

    public Instant getLastModified() {
        return lastModified;
    }

    public long getUncompressedSize() {
        return uncompressedSize;
    }

    public long getCompressedSize() {
        return compressedSize;
    }

    public String getCompressionPercentage() {
        final double ratio = (double)compressedSize / (double)uncompressedSize;
        final double percentage = ratio * 100;
        return Precision.round(percentage, 1) + "%";
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sourcePath", sourcePath)
                .append("tempPath", tempPath)
                .append("checksum", Hex.toHexString(checksum))
                .append("lastModified", lastModified)
                .append("compressionPercentage", getCompressionPercentage())
                .append("uncompressedSize", uncompressedSize)
                .append("compressedSize", compressedSize)
                .toString();
    }
}
