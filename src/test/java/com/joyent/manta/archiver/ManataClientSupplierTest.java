/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. If a copy of the MPL was not
 * distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.joyent.http.signature.KeyLoadException;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.exception.ConfigurationException;

@Test(singleThreaded = true)
public class ManataClientSupplierTest {

    private static final MantaClientSupplier MANTA_CLIENT_SUPPLIER = new MantaClientSupplier();
    private File keyFile;
    public static final String MANTA_URL_PROPERTY = "manta.url";
    private String keyId;

    public ManataClientSupplierTest() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        keyFile = new File(classLoader.getResource("keys/rsa/ValidKey.ida").getFile());
        File keyIdFile = new File(classLoader.getResource("keys/rsa/ValidKey_id.txt").getFile());
        Path path = FileSystems.getDefault().getPath(keyIdFile.getParentFile().getPath(), "ValidKey_id.txt");
        keyId = new String(Files.readAllBytes(path)).trim();
    }

    @BeforeMethod
    private void setParameters() {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_URL_KEY, "");
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_USER_KEY, "JunkUser");
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_KEY_ID_KEY, keyId);
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_KEY_PATH_KEY, keyFile.getAbsolutePath());
    }

    @Test
    public void basicTest() {
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test(expectedExceptions = {
            KeyLoadException.class }, expectedExceptionsMessageRegExp = ".*testKey.ida.*", description = "This will create a empty temp file and set the property to point to it")
    public void emptyKeyFile() throws IOException {
        String keyfile = "testKey.ida";
        keyfile = String.format("%s%s", System.getProperty("java.io.tmpdir"), keyfile);
        File keyFile = new File(keyfile);
        keyFile.createNewFile();
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_KEY_PATH_KEY, keyFile.getAbsolutePath());
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNull(client);
    }

    /**
     * This will have an invalid key file, but not empty.
     *
     * @throws IOException
     */
    @Test(expectedExceptions = {
            KeyLoadException.class }, expectedExceptionsMessageRegExp = ".*invalidKey.ida.*", description = "This will create a non-empty invalid key")
    public void invalidKey() throws IOException {
        String filename = "invalidKey.ida";
        filename = String.format("%s%s", System.getProperty("java.io.tmpdir"), filename);
        File keyFile = new File(filename);
        keyFile.createNewFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
            byte[] array = new byte[128];
            new Random().nextBytes(array);
            String generatedString = new String(array, Charset.forName("UTF-8"));
            bw.write(generatedString);
        } catch (Exception e) {
            Assert.fail("Failed on unexpected exception " + e.getMessage());
        }
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_KEY_PATH_KEY, keyFile.getAbsolutePath());
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNull(client);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*protocol = null.*", description = "This will use an invalid URL")
    public void invalidURL() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_URL_KEY, "www.google.com");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void emptyURL() {
        System.clearProperty(com.joyent.manta.config.MapConfigContext.MANTA_URL_KEY);
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        // Should default to us-east.
        Assert.assertEquals(client.getContext().getMantaURL(), "https://us-east.manta.joyent.com");
    }

    @Test
    public void invalidUser() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_USER_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidPassword() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_USER_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void settingNoAuth() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_NO_AUTH_KEY, "true");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidValueVerifyUpload() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_VERIFY_UPLOADS_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidTimeout() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_TIMEOUT_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidHTTPRetries() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_RETRIES_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidMaxConnections() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_MAX_CONNS_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidBufferSize() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_HTTP_BUFFER_SIZE_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidSocketTimeout() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_TCP_SOCKET_TIMEOUT_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidUploadBufferSize() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_UPLOAD_BUFFER_SIZE_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidEncryptionAutMode() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_ENCRYPTION_AUTHENTICATION_MODE_KEY,
                "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test
    public void invalidEncryptionAlgorithm() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_ENCRYPTION_ALGORITHM_KEY,
                "SomethingNotRight");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

    @Test(expectedExceptions = {
            ConfigurationException.class }, expectedExceptionsMessageRegExp = "Given fingerprint invalid does not match expected key.*")
    public void invalidKeyId() throws IOException {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_KEY_ID_KEY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Assert.assertNotNull(client);
    }

}
