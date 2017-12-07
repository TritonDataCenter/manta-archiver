/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Test
public class TransferManagerTest {
    public void canUploadAll() throws IOException {
        final TransferClient client = new EchoTransferClient();
        Path root = Files.createTempDirectory("archiver-");
        FakeDirectoryStructureCreator.createFakeDirectoriesAndFiles(root);
        final String mantaRoot = "/home/username/stor/backup";

        try (TransferManager manager = new TransferManager(client, root, mantaRoot)) {
            manager.uploadAll();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
