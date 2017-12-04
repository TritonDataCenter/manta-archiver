/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.joyent.manta.client.MantaClient;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test
public class MantaTransferClientTest {
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

    private void assertMantaPathConversionEquals(final ObjectUpload upload,
                                                 final String expectedRelativePath) {
        final String mantaRoot = "/username/stor/backup";
        final TransferClient client = new MantaTransferClient(null, mantaRoot);
        final Path root = Paths.get("/var");

        String actual = client.convertLocalPathToRemotePath(upload, root);
        String expected = mantaRoot + MantaClient.SEPARATOR + expectedRelativePath;
        assertEquals(actual, expected);
    }
}
