/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

/**
 * Enum capturing the various verification statuses possible.
 */
public enum VerificationResult {
    /**
     * Local and remote objects match.
     */
    OK,
    /**
     * Local and remote links match.
     */
    LINK_OK,
    /**
     * Remote object is missing the headers needed to verify.
     */
    MISSING_HEADERS,
    /**
     * Remote object doesn't exist.
     */
    NOT_FOUND,
    /**
     * Local object is a directory but remote object is a file.
     */
    NOT_DIRECTORY,
    /**
     * Local object is a file but remote object is a directory.
     */
    NOT_FILE,
    /**
     * Local object is a link but remote object is file and not a link.
     */
    NOT_LINK_ACTUALLY_FILE,
    /**
     * Local object is a link but remote object is empty directory and not a link.
     */
    NOT_LINK_ACTUALLY_EMPTY_DIR,
    /**
     * Local object is a link but remote object is a directory and not a link.
     */
    NOT_LINK_ACTUALLY_DIR,
    /**
     * Local object size does not match remote object size.
     */
    WRONG_SIZE,
    /**
     * Local object checksum does not match remote object checksum.
     */
    CHECKSUM_MISMATCH,
    /**
     * Local link path is different that remote link path.
     */
    LINK_MISMATCH;

    /**
     * Maximum size of enum as string (used for centering text).
     */
    public static final int MAX_STRING_SIZE = 19;

    /**
     * @return true if verification is successful
     */
    public boolean isOk() {
        return this.equals(OK) || this.equals(LINK_OK);
    }

    /**
     * @return true if verification indicates that we failed because we didn't
     *         have a link present remotely
     */
    public boolean isNotLink() {
        return this.equals(NOT_LINK_ACTUALLY_FILE)
                || this.equals(NOT_LINK_ACTUALLY_EMPTY_DIR)
                || this.equals(NOT_LINK_ACTUALLY_DIR);
    }
}
