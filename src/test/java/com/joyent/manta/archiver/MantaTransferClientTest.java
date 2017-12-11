/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.joyent.manta.client.MantaClient;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test
public class MantaTransferClientTest {
    private static final String MANTA_ROOT = "/username/stor/backup";

    public void canConvertFromUnixFilePathToMantaPath() throws IOException {
        ObjectUpload upload = mock(ObjectUpload.class);
        Path file = Files.createTempFile("test-file", ".data");
        FileUtils.forceDeleteOnExit(file.toFile());

        when(upload.getSourcePath()).thenReturn(file);

        String expected = file.subpath(1, file.getNameCount()).toString()
                + "." + ObjectCompressor.COMPRESSION_TYPE;

        assertMantaPathConversionEquals(upload, expected);
    }

    public void canConvertFromUnixDirectoryPathToMantaPath() throws IOException {
        ObjectUpload upload = mock(ObjectUpload.class);
        Path dir = Files.createTempDirectory("test-dir");
        FileUtils.forceDeleteOnExit(dir.toFile());

        when(upload.isDirectory()).thenReturn(true);
        when(upload.getSourcePath()).thenReturn(dir);

        String expected = dir.subpath(1, dir.getNameCount()).toString() + "/";

        assertMantaPathConversionEquals(upload, expected);
    }

    private void assertMantaPathConversionEquals(final ObjectUpload upload,
                                                 final String expectedRelativePath) {
        final TransferClient client = new MantaTransferClient(null, MANTA_ROOT);
        final Path root = Paths.get(System.getProperty("java.io.tmpdir"));

        String actual = client.convertLocalPathToRemotePath(upload.getSourcePath(),
                root);
        String expected = MANTA_ROOT + MantaClient.SEPARATOR + expectedRelativePath;
        assertEquals(actual, expected);
    }
}
