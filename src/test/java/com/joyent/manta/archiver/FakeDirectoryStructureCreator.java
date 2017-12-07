/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class FakeDirectoryStructureCreator {
    private static final File TEMPLATE_FAKE_FILE;

    static {
        try {
            TEMPLATE_FAKE_FILE = Files.createTempFile(
                    "maven-archiver-template-", ".binary").toFile();

            FileUtils.forceDeleteOnExit(TEMPLATE_FAKE_FILE);

            try (FileOutputStream out = new FileOutputStream(TEMPLATE_FAKE_FILE)) {
                for (int z = 0; z < 10_000; z++) {
                    out.write((byte)0);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void createFakeDirectoriesAndFiles(final Path root) throws IOException {
        int subdirectories = 9;
        int filesPerDirectory = 200;

        Path last = root;
        for (int i = 0; i < subdirectories; i++) {
            Path dir = last.resolve("dir-" + i);
            dir.toFile().mkdirs();

            for (int y = 0; y < filesPerDirectory; y++) {
                fakeFile(dir.resolve("file-" + y).toFile());
            }

            if (i == subdirectories -1) {
                last = dir;
            }
            System.out.println(dir);
        }

        FileUtils.forceDeleteOnExit(root.toFile());
    }

    static File fakeFile(final File file) throws IOException {
        FileUtils.copyFile(TEMPLATE_FAKE_FILE, file);
        return file;
    }
}
