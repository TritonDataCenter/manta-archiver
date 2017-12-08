/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Utility class containing directory utility methods.
 */
final class DirectoryUtils {
    /**
     * Private constructor for utility class.
     */
    private DirectoryUtils() {
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
}
