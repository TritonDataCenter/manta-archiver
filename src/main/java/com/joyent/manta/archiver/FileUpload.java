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
class FileUpload implements ObjectUpload {
    private final Path sourcePath;
    private final Path tempPath;
    private final byte[] checksum;
    private final Instant lastModified;
    private final long uncompressedSize;
    private final long compressedSize;

    /**
     * Creates a new instance of a file object.
     *
     * @param tempPath path to the compressed version of the file
     * @param sourcePath path to the original uncompressed version of the file
     * @param checksum checksum of the original uncompressed version of the file
     * @param lastModified last-modified timestamp
     * @param uncompressedSize size of the file uncompressed
     * @param compressedSize size of the file compressed
     */
    FileUpload(final Path tempPath, final Path sourcePath,
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

    Path getTempPath() {
        return tempPath;
    }

    byte[] getChecksum() {
        return checksum;
    }

    Instant getLastModified() {
        return lastModified;
    }

    long getUncompressedSize() {
        return uncompressedSize;
    }

    long getCompressedSize() {
        return compressedSize;
    }

    /**
     * @return percentage in which the file was compressed from the original size
     */
    String getCompressionPercentage() {
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
