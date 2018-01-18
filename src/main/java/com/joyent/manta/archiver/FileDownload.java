/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Objects;

/**
 * Class that provides properties about a file that is to be downloaded.
 */
class FileDownload implements Serializable {
    private static final long serialVersionUID = -5846664894766812628L;

    private final Long size;
    private final Long lastModified;
    private final String remotePath;
    private final boolean isDirectory;
    private Boolean isLink;

    /**
     * Creates a new instance.
     *
     * @param size uncompressed size of file to download
     * @param lastModified remote last modified time in epoch milliseconds
     * @param remotePath remote Manta path
     * @param isDirectory flag indicating if the object is a directory
     */
    FileDownload(final Long size,
                 final Long lastModified,
                 final String remotePath,
                 final boolean isDirectory,
                 final Boolean isLink) {
        this.size = size;
        this.lastModified = lastModified;
        this.remotePath = remotePath;
        this.isDirectory = isDirectory;
        this.isLink = isLink;
    }

    /**
     * Creates a new instance.
     *
     * @param size uncompressed size of file to download
     * @param lastModified remote last modified time in epoch milliseconds
     * @param remotePath remote Manta path
     */
    FileDownload(final Long size,
                 final Long lastModified,
                 final String remotePath,
                 final boolean isDirectory) {
        this(size, lastModified, remotePath, isDirectory, null);
    }

    long getSize() {
        return size;
    }

    long getLastModified() {
        return lastModified;
    }

    String getRemotePath() {
        return remotePath;
    }

    boolean isDirectory() {
        return isDirectory;
    }

    boolean isLink() {
        return BooleanUtils.toBoolean(isLink);
    }

    FileDownload setLink(final boolean link) {
        isLink = link;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FileDownload that = (FileDownload) o;
        return size == that.size
                && lastModified == that.lastModified
                && Objects.equals(remotePath, that.remotePath)
                && Objects.equals(isDirectory, that.isDirectory)
                && Objects.equals(isLink, that.isLink);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, lastModified, remotePath, isDirectory, isLink);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("size", size)
                .append("lastModified", lastModified)
                .append("remotePath", remotePath)
                .append("isDirectory", isDirectory)
                .append("isLink", isLink)
                .toString();
    }
}
