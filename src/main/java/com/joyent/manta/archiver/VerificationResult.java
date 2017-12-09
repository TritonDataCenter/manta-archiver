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
     * Local object size does not match remote object size.
     */
    WRONG_SIZE,
    /**
     * Local object checksum does not match remote object checksum.
     */
    CHECKSUM_MISMATCH
}
