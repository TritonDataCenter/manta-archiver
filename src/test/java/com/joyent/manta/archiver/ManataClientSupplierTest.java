/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. If a copy of the MPL was not
 * distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
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

    public static final String MANTA_URL = "MANTA_URL";
    public static final String MANTA_USER = "MANTA_USER";
    public static final String MANTA_KEY_ID = "MANTA_KEY_ID";
    public static final String MANTA_KEY_PATH = "MANTA_KEY_PATH";

    private File keyFile;

    public static final String MANTA_URL_PROPERTY = "manta.url";

    public ManataClientSupplierTest() { 
        String keyFileName = "ValidButNotRegistered.ida";
               keyFileName = String.format("%s%s", System.getProperty("java.io.tmpdir"), keyFileName);
                keyFile = new File(keyFileName);
                try (BufferedWriter bw = new BufferedWriter(new FileWriter(keyFileName))) {
                    keyFile.createNewFile();
                    String validKey = "-----BEGIN RSA PRIVATE KEY-----\n"
                            + "MIIEpAIBAAKCAQEAqPX5IXovNwXe5ouz0TCekkdDNXS7E5dIZLmjRK4TxQAbulPj\n"
                            + "Y0/FVclIDpjwVFw1mCdCPVNk47/++kUDAl8Gl0EUrGqeDe/Q5eK234oavz3oylWd\n"
                            + "elmYqApH8fplMAVfeur8e+5pQHB2FLCcsgEK+3zRtfI7vgXY6xE3WUGlCmNjN/wH\n"
                            + "jccn8G11RMibIpkFuE/g6OBm0GaC8Ikjn5lpVQcLTqRxHyHHo/0jE9rUy8IvVonb\n"
                            + "ZaSlx7clwm6szN6aE5/FCEwaGsSxdlkzgPDc+0pO+GTdwhGs7Rl2ExcLhSWnPjUA\n"
                            + "ZuZLcmFa4adD+NQHbTw2HlLqaHRJV4g0BsY2oQIDAQABAoIBAQCnzaLf3LmHrAz0\n"
                            + "a0rrN55FKQFW1df2XQlJABVm4HxB6xmetDHhMBiMWpt14+7L2chglJz0yx4oE0bo\n"
                            + "yCF0+WtSTRB7LGhM7yBJMCDvYfmudY39ZYpBOTqjqZJKgKR+TNfG/BpF+0IM/aRI\n"
                            + "aB83qlF98zlTuoAa+TO2QiL2QnvnE7CyTn1uhhzRCT2YTuwUECqSTNHHHQrgGyCE\n"
                            + "GD7YlzckUt1GD3cOKKUt9BE6/i2TjMSkC6N+Az1T8uzybENWupAqizcCws2jFtXS\n"
                            + "iJmWzB3BtLHEVZN1D56wQTY2Q2FTT/iFbLDLt8Y+YW6jC1F03iHhyqaseZeUSKuS\n"
                            + "/xwucDABAoGBANlvN74e11cZLLhmn5tbzZFUWlUcyO0x5uF84aIr9uGnZdlhj5Xt\n"
                            + "84HzWSavbFAiRpuHwY6VudAq7cRAx+P5pCWSH3soS0kThd15p3LoEibPi3ad8gq6\n"
                            + "vAhNEPkEqkBoZVRj4VgpweV7gdIJbIZ8JbZPU+Dq9PetseRZqqYMoyvJAoGBAMbt\n"
                            + "xdxzM7xcQ3L7T37CBt251G3Dmcuw+fbp/dWWCKqvWjQX8yMRVtlcd28+XHyJiVOt\n"
                            + "vjOaJgHS9/IZVwd95zv5dtfKYSk7tivtOD/YJ8FxprqtfMyxnIz2wFhWsawPuDjX\n"
                            + "aVf4Q0oP46MBiRIQgWvMraYJtMafssnSpz/Dt3AZAoGBAJ6vGVpqPbQ2Djohwzfp\n"
                            + "vtPiYO6ezFC3S42iyzTEqy+yMJV+KwE7oKxlQdoGyqCM80TMxcjeorY2rkG9GWTa\n"
                            + "qx40Tz9df9w8IEUrZLZqgdzLOTf/O0bzUwkn3UwvSGUrC1CUeEAUcYqeIXd9IzPe\n"
                            + "5NLxgAC02MTtgddqTS1UKb1pAoGANgYAw/utQywzTRie4CfFQZXj8OM78yte1wV4\n"
                            + "3/Zc6C2y647NguqEkYchEF75MwEPAGCg1Na6F6i5mU/0aJ5ym8EF21ikxlPnB0rn\n"
                            + "Cb+kHE7HHs9aoyRhBY9FcTgqDDZAq38kprVPYN+rzGrwVK2S2Dm/tuXP6FkabuD8\n"
                            + "dr6qJJECgYBKc09htDe/OBPqyVtEuUv0HTA5KRNpqeheWCKTDAFtjpciYa1qLyjs\n"
                            + "JET3gPfjWkcuDOCs6zcaj/anlr/PxdSxYw4Zu21YKZGY70ceBEJgtX5PcrgmpH7R\n"
                            + "99+DOAhlNqwK5X+4Foo3tErRveaTczgz38G17IfoKJ7rKYHQt1o+bQ==\n" + "-----END RSA PRIVATE KEY-----";
                    bw.write(validKey);
                } catch (Exception e) {
                   Assert.fail("Failed while writing key file.");
                }
    }

    @BeforeMethod
    private void setParameters() {
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_URL_KEY, "");
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_USER_KEY, "JunkUser");
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_KEY_ID_KEY, "MD5:73:a4:21:e9:1c:95:bf:31:5d:14:0b:83:3c:a8:9b:20");
        System.setProperty(com.joyent.manta.config.MapConfigContext.MANTA_KEY_PATH_KEY, keyFile.getAbsolutePath());
    }

    @Test
    public void testSomething() {
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
