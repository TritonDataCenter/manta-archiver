/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

/**
 * Class that represents a symbolic link to be added to Manta.
 */
class SymbolicLinkUpload implements ObjectUpload {
    private final Path sourcePath;

    /**
     * Creates a new instance representing link in the specified path.
     *
     * @param sourcePath path to symbolic link
     */
    SymbolicLinkUpload(final Path sourcePath) {
        Validate.isTrue(Files.isSymbolicLink(sourcePath),
                "Specified path %s is not a symbolic link",
                sourcePath);
        this.sourcePath = sourcePath;
    }

    @Override
    public Path getSourcePath() {
        return sourcePath;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    /**
     * @return returns the path of a symbolic link
     */
    Path resolvedPath() {
        try {
            return Files.readSymbolicLink(sourcePath);
        } catch (IOException e) {
            String msg = String.format("An unknown issue occurred when reading "
                + "a symlink at: %s", sourcePath);
            throw new UncheckedIOException(msg, e);
        }
    }

    /**
     * @return returns the last modified time of a symbolic link
     */
    Instant getLastModified() {
        try {
            return Files.getLastModifiedTime(sourcePath, LinkOption.NOFOLLOW_LINKS)
                    .toInstant();
        } catch (IOException e) {
            String msg = String.format("An unknown issue occurred when reading "
                    + "a symlink at: %s", sourcePath);
            throw new UncheckedIOException(msg, e);
        }
    }

    /**
     * @return returns the byte representation of the symbolic link target path
     */
    byte[] linkPathAsUtf8() {
        final Path resolved = resolvedPath();
        return resolved.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SymbolicLinkUpload upload = (SymbolicLinkUpload) o;
        return Objects.equals(sourcePath, upload.sourcePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourcePath);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sourcePath", sourcePath)
                .toString();
    }
}
