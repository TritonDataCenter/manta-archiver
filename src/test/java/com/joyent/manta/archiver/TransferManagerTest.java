/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test
public class TransferManagerTest {
    public void canUploadAll() {
        final TransferClient client = new EchoTransferClient();
        final Path root = Paths.get("/opt/duck");
        final String mantaRoot = "/home/username/stor/backup";

        try (TransferManager manager = new TransferManager(client, root, mantaRoot)) {
            manager.uploadAll();
        }
    }

    public void canConvertFromUnixFilePathToMantaPath() {
        ObjectUpload upload = mock(ObjectUpload.class);
        when(upload.getSourcePath()).thenReturn(Paths.get("/var/app/bar"));
        assertMantaPathConversionEquals(upload, "app/bar.xz");
    }

    public void canConvertFromUnixDirectoryPathToMantaPath() {
        ObjectUpload upload = mock(ObjectUpload.class);
        when(upload.isDirectory()).thenReturn(true);
        when(upload.getSourcePath()).thenReturn(Paths.get("/var/app/bar"));
        assertMantaPathConversionEquals(upload, "app/bar/");
    }

    public void canTransferToManta() {
        final ConfigContext config = new SystemSettingsConfigContext();
        final MantaClient mantaClient = new MantaClient(config);
        final Path root = Paths.get("/opt/duck");
        final String mantaRoot = "/elijah.zupancic/stor/archive-test";
        final TransferClient client = new MantaTransferClient(mantaClient, mantaRoot);

        try (TransferManager manager = new TransferManager(client, root, mantaRoot)) {
            manager.uploadAll();
        }
    }

    private void assertMantaPathConversionEquals(final ObjectUpload upload,
                                                 final String expectedRelativePath) {
        final TransferClient client = new EchoTransferClient();
        final Path root = Paths.get("/var");
        final String mantaRoot = "/username/stor/backup";

        try (TransferManager manager = new TransferManager(client, root, mantaRoot)) {
            String actual = manager.convertToMantaPath(upload);
            String expected = mantaRoot + MantaClient.SEPARATOR + expectedRelativePath;
            assertEquals(actual, expected);
        }
    }
}
