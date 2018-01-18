/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.twmacinta.util.MD5;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Utility class containing directory and file utility methods.
 */
final class LocalFileUtils {
    /**
     * Private constructor for utility class.
     */
    private LocalFileUtils() {
    }

    /**
     * Stream of an entire recursive directory structure for the specified path.
     *
     * @param root root to traverse
     * @return stream of directory contents
     */
    static Stream<Path> directoryContentsStream(final Path root) {
        try {
            return Files.walk(root);
        } catch (IOException e) {
            String msg = "Unable to recursively traverse path";
            FileProcessingException fpe = new FileProcessingException(msg, e);
            fpe.setContextValue("rootPath", root);

            throw fpe;
        }
    }

    /**
     * Performs checksum for the given path.
     *
     * @param path path to generate checksum for
     * @return checksum signature as a byte array
     */
    static byte[] checksum(final Path path) {
        try {
            return MD5.getHash(path.toFile());
        } catch (IOException e) {
            String msg = "Error reading file from path";
            FileProcessingException fpe = new FileProcessingException(msg, e);
            fpe.setContextValue("path", path);

            throw fpe;
        }
    }
}
