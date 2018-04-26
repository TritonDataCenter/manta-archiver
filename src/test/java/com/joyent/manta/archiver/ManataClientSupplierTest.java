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
import java.util.Iterator;
import java.util.Random;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.joyent.http.signature.KeyLoadException;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.exception.ConfigurationException;
import com.joyent.manta.exception.MantaClientHttpResponseException;

import jline.internal.Log;

@Test
public class ManataClientSupplierTest {
    private static Logger LOG = LoggerFactory.getLogger(ManataClientSupplierTest.class);

    private static final MantaClientSupplier MANTA_CLIENT_SUPPLIER = new MantaClientSupplier();
    // Seems silly, but what if they change?
    public static final String MANTA_URL = "MANTA_URL";
    public static final String MANTA_USER = "MANTA_USER";
    public static final String MANTA_KEY_ID = "MANTA_KEY_ID";
    public static final String MANTA_KEY_PATH = "MANTA_KEY_PATH";

    public static final String MANTA_URL_PROPERTY = "manta.url";
    public static final String MANTA_USER_PROPERTY = "manta.user";
    public static final String MANTA_KEY_ID_PROPERTY = "manta.key_id";
    public static final String MANTA_KEY_PATH_PROPERTY = "manta.key_path";
    public static final String MANTA_PASSWORD_PROPERTY = "manta.password";

    public static final String MANTA_KEY_CONTENT = "manta.key_content";
    public static final String MANTA_PASSWORD = "manta.password";
    public static final String MANTA_NO_AUTH = "manta.no_auth";
    public static final String MANTA_NO_NATIVE_SIGS = "manta.disable_native_sigs";
    public static final String MANTA_VERIFY_UPLOAD = "manta.verify_uploads";
    public static final String MANTA_TIMEOUT = "manta.timeout";
    public static final String MANTA_HTTP_RETRIES = "manta.retries";
    public static final String MANTA_MAX_CONNS = "manta.max_connections";
    public static final String MANTA_HTTP_BUFFER_SIZE = "manta.http_buffer_size";
    public static final String MANTA_HTTPS_PROTOCOLS = "https.protocols";
    public static final String MANTA_HTTPS_CIPHERS = "https.cipherSuites";
    public static final String MANTA_TCP_SOCKET_TIMEOUT = "manta.tcp_socket_timeout";
    public static final String MANTA_CONNECTION_REQUEST_TIMEOUT = "manta.connection_request_timeout";
    public static final String MANTA_EXPECT_CONTINUE_TIMEOUT = "manta.expect_continue_timeout";
    public static final String MANTA_UPLOAD_BUFFER_SIZE = "manta.upload_buffer_size";
    public static final String MANTA_SKIP_DIRECTORY_DEPTH = "manta.skip_directory_depth";
    public static final String MANTA_CLIENT_ENCRYPTION = "manta.client_encryption";
    public static final String MANTA_CLIENT_ENCRYPTION_KEY_ID = "manta.encryption_key_id";
    public static final String MANTA_ENCRYPTION_ALGORITHM = "manta.encryption_algorithm";
    public static final String MANTA_UNENCRYPTED_DOWNLOADS = "manta.permit_unencrypted_downloads";
    public static final String MANTA_ENCRYPTION_AUTH_MODE = "manta.encryption_auth_mode";
    public static final String MANTA_ENCRYPTION_KEY_PATH = "manta.encryption_key_path";
    public static final String MANTA_ENCRYPTION_KEY_BYTES_64 = "manta.encryption_key_bytes_base";

    private final String manta_user = System.getenv(MANTA_USER);
    private static final String base_dir = String.format("/%s", System.getenv(MANTA_USER));
    private static final String stor_dir = String.format("/%s/stor", System.getenv(MANTA_USER));

    @BeforeMethod
    private void setParameters() {
        System.setProperty(MANTA_URL_PROPERTY, System.getenv(MANTA_URL));
        System.setProperty(MANTA_USER_PROPERTY, System.getenv(MANTA_USER));
        System.setProperty(MANTA_KEY_ID_PROPERTY, System.getenv(MANTA_KEY_ID));
        System.setProperty(MANTA_KEY_PATH_PROPERTY, System.getenv(MANTA_KEY_PATH));
    }

    // @Test(expectedExceptions = {
    // KeyLoadException.class }, expectedExceptionsMessageRegExp = ".*testKey.ida.*", description = "This will create a
    // empty temp file and set the property to point to it")
    // public void emptyKeyFile() throws IOException {
    // String keyfile = "testKey.ida";
    // keyfile = String.format("%s%s", System.getProperty("java.io.tmpdir"), keyfile);
    // File keyFile = new File(keyfile);
    // keyFile.createNewFile();
    //
    // System.setProperty(MANTA_KEY_PATH_PROPERTY, keyFile.getAbsolutePath());
    // MantaClient client = MANTA_CLIENT_SUPPLIER.get();
    // }

    /**
     * This will have an invalid key file, but not empty.
     *
     * @throws IOException
     */
    // @Test(expectedExceptions = {
    // KeyLoadException.class }, expectedExceptionsMessageRegExp = ".*invalidKey.ida.*", description = "This will create
    // a non-empty invalid key")
    // public void invalidKey() throws IOException {
    // String filename = "invalidKey.ida";
    // filename = String.format("%s%s", System.getProperty("java.io.tmpdir"), filename);
    // File keyFile = new File(filename);
    // keyFile.createNewFile();
    // try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
    // byte[] array = new byte[128];
    // new Random().nextBytes(array);
    // String generatedString = new String(array, Charset.forName("UTF-8"));
    // bw.write(generatedString);
    // } catch (Exception e) {
    // Assert.fail("Failed on unexpected exception " + e.getMessage());
    // }
    // System.setProperty(MANTA_KEY_PATH_PROPERTY, keyFile.getAbsolutePath());
    // MantaClient client = MANTA_CLIENT_SUPPLIER.get();
    // Stream<MantaObject> mor = client.listObjects("/" + manta_user + "/stor");
    // Iterator<MantaObject> it = mor.iterator();
    // while (it.hasNext()) {
    // Log.info(it.next());
    // }
    // }

    /**
     * This will use a key file that has a valid key but not one currently owned by the user.
     * 
     * @throws Exception
     */
    @Test(expectedExceptions = {
            ConfigurationException.class }, expectedExceptionsMessageRegExp = ".*Given fingerprint.*", description = "This will use a valid key that is not registered to the user.")
    public void notRegisteredKey() throws Exception {
        String keyFileName = "ValidButNotRegistered.ida";
        keyFileName = String.format("%s%s", System.getProperty("java.io.tmpdir"), keyFileName);
        File keyFile = new File(keyFileName);
        keyFile.createNewFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(keyFileName))) {
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
            throw e;
        }
        System.setProperty(MANTA_KEY_PATH_PROPERTY, keyFile.getAbsolutePath());
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects("/" + System.getProperty(MANTA_USER) + "/stor");
        Iterator<MantaObject> it = mor.iterator();
        while (it.hasNext()) {
            Log.info(it.next());
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*protocol = null.*", description = "This will use an invalid URL")
    public void invalidURL() throws IOException {
        System.setProperty(MANTA_URL_PROPERTY, "www.google.com");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.fail(String.format("Successfully made a call that we should not be able to call, found : %d objects",
                mor.count()));
    }

    @Test
    public void emptyURL() {
        System.clearProperty(MANTA_URL_PROPERTY);
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        // Should default to us-east.
        Assert.assertEquals(client.getContext().getMantaURL(), "https://us-east.manta.joyent.com");
    }

    @Test(expectedExceptions = MantaClientHttpResponseException.class, expectedExceptionsMessageRegExp = ".*HTTP request failed to.*")
    public void invalidUser() throws IOException {
        System.setProperty(MANTA_USER_PROPERTY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Seeing more than 0 entries, This should have thrown an exception by now.");
    }

    @Test(expectedExceptions = MantaClientHttpResponseException.class, expectedExceptionsMessageRegExp = ".*HTTP request failed to:.*")
    public void invalidPassword() throws IOException {
        System.setProperty(MANTA_USER_PROPERTY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.fail(String.format("Successfully made a call that we should not be able to call, found : %d objects",
                mor.count()));
    }

    @Test(expectedExceptions = MantaClientHttpResponseException.class, expectedExceptionsMessageRegExp = ".*HTTP request failed to:.*")
    public void settingNoAuth() throws IOException {
        System.setProperty(MANTA_NO_AUTH, "true");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.fail(String.format("Successfully made a call that we should not be able to call, found : %d objects",
                mor.count()));
    }

    @Test
    public void invalidValueVerifyUpload() throws IOException {
        System.setProperty(MANTA_VERIFY_UPLOAD, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test
    public void invalidTimeout() throws IOException {
        System.setProperty(MANTA_TIMEOUT, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test
    public void invalidHTTPRetries() throws IOException {
        System.setProperty(MANTA_HTTP_RETRIES, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test
    public void invalidMaxConnections() throws IOException {
        System.setProperty(MANTA_MAX_CONNS, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test
    public void invalidBufferSize() throws IOException {
        System.setProperty(MANTA_HTTP_BUFFER_SIZE, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test
    public void invalidSocketTimeout() throws IOException {
        System.setProperty(MANTA_TCP_SOCKET_TIMEOUT, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test
    public void invalidUploadBufferSize() throws IOException {
        System.setProperty(MANTA_UPLOAD_BUFFER_SIZE, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test
    public void invalidEncryptionAutMode() throws IOException {
        System.setProperty(MANTA_ENCRYPTION_AUTH_MODE, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test
    public void invalidEncryptionAlgorithm() throws IOException {
        System.setProperty(MANTA_ENCRYPTION_ALGORITHM, "SomethingNotRight");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.assertTrue(mor.count() > 0, "Count returned 0, it should be greater than 0");
    }

    @Test(expectedExceptions = ConfigurationException.class, expectedExceptionsMessageRegExp = ".*Given fingerprint invalid does not match expected key.*")
    public void invalidKeyId() throws IOException {
        System.setProperty(MANTA_KEY_ID_PROPERTY, "invalid");
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
        Stream<MantaObject> mor = client.listObjects(base_dir);
        Assert.fail(String.format("Successfully made a call that we should not be able to call, found : %d objects",
                mor.count()));
    }

}
