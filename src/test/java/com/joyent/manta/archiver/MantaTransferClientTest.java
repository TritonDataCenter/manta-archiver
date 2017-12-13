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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test
public class MantaTransferClientTest {
    private static final String MANTA_ROOT = "/username/stor/backup";
    private static final String LOCAL_ROOT =
            FilenameUtils.normalize(System.getProperty("java.io.tmpdir")
            + "manta-transfer-client-test");

    @BeforeClass
    public void before() throws IOException {
        File localRoot = new File(LOCAL_ROOT);
        localRoot.mkdirs();
        FileUtils.forceDeleteOnExit(localRoot);
    }

    public void canConvertFromFilePathToMantaPath() throws IOException {
        ObjectUpload upload = mock(ObjectUpload.class);
        String filename = String.format("test-file-%s.data", UUID.randomUUID());
        Path file = Paths.get(LOCAL_ROOT, filename);

        when(upload.getSourcePath()).thenReturn(file);

        String relative = StringUtils.removeFirst(file.toAbsolutePath().toString(), LOCAL_ROOT);
        String noLeadingSeparator = StringUtils.removeFirst(relative, File.separator);
        String expected = FilenameUtils.normalize(noLeadingSeparator + "."
                + ObjectCompressor.COMPRESSION_TYPE);

        assertMantaPathConversionEquals(upload, expected);
    }

    public void canConvertFromDirectoryPathToMantaPath() throws IOException {
        ObjectUpload upload = mock(ObjectUpload.class);
        Path dir = Paths.get(LOCAL_ROOT, "test-dir-" + UUID.randomUUID());
        dir.toFile().mkdirs();

        when(upload.isDirectory()).thenReturn(true);
        when(upload.getSourcePath()).thenReturn(dir);

        String relative = StringUtils.removeFirst(dir.toAbsolutePath().toString(), LOCAL_ROOT);
        String noLeadingSeparator = StringUtils.removeFirst(relative, File.separator);
        String expected = FilenameUtils.normalize(noLeadingSeparator + MantaClient.SEPARATOR);

        assertMantaPathConversionEquals(upload, expected);
    }

    public void canConvertFromMantaPathToUnixPath() throws IOException {
        final String remotePath = MANTA_ROOT + MantaClient.SEPARATOR + "a-single-file."
                + ObjectCompressor.COMPRESSION_TYPE;

        final Path expected = Paths.get(LOCAL_ROOT, "a-single-file");

        assertMantaPathConversionEquals(remotePath, expected);
    }

    private void assertMantaPathConversionEquals(final ObjectUpload upload,
                                                 final String expectedRelativePath) {
        final TransferClient client = new MantaTransferClient(null, MANTA_ROOT);
        final Path root = Paths.get(LOCAL_ROOT);

        String actual = client.convertLocalPathToRemotePath(upload.getSourcePath(),
                root);
        String expected = MANTA_ROOT + MantaClient.SEPARATOR + expectedRelativePath;
        assertEquals(actual, expected);
    }

    private void assertMantaPathConversionEquals(final String remotePath, final Path expected) {
        final TransferClient client = new MantaTransferClient(null, MANTA_ROOT);
        final Path root = Paths.get(LOCAL_ROOT);

        Path actual = client.convertRemotePathToLocalPath(remotePath, root);
        assertEquals(actual, expected);
    }
}
