package com.joyent.manta.archiver;/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

@Test
public class TransferManagerIT {
    private static final Supplier<MantaClient> MANTA_CLIENT_SUPPLIER =
            new MantaClientSupplier();

    public void canTransferToManta() {
        final ConfigContext config = new SystemSettingsConfigContext();
        final MantaClient mantaClient = new MantaClient(config);
        final Path root = Paths.get("/opt/duck");
        final String mantaRoot = "/elijah.zupancic/stor/archive-test";
        final TransferClient client = new MantaTransferClient(MANTA_CLIENT_SUPPLIER, mantaRoot);

        try (TransferManager manager = new TransferManager(client, root, mantaRoot)) {
            manager.uploadAll();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
