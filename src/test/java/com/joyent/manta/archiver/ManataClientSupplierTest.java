/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.Test;

import com.joyent.http.signature.KeyLoadException;
import com.joyent.manta.client.MantaClient;

@Test
public class ManataClientSupplierTest {
    private static final MantaClientSupplier MANTA_CLIENT_SUPPLIER =
            new MantaClientSupplier();
    
    @Test(expectedExceptions = { KeyLoadException.class }, expectedExceptionsMessageRegExp = "testKey.ida")
    public void emptyKeyFile() throws IOException {
     // Writing an empty
        String filename = "testKey.ida";
        filename = String.format("%s%s", System.getProperty("java.io.tmpdir"), filename);
        File emptyFile = new File(filename);
        emptyFile.createNewFile();
        
        System.setProperty("MANTA_KEY_PATH", emptyFile.getAbsolutePath());
        MantaClient client = MANTA_CLIENT_SUPPLIER.get();
    }
}
