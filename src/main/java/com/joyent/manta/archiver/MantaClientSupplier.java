/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EnvVarConfigContext;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.config.StandardConfigContext;

import java.util.function.Supplier;

/**
 * Provides configured {@link MantaClient} instances.
 */
public class MantaClientSupplier implements Supplier<MantaClient> {
    private static final int EXPECT_CONTINUE_DEFAULT_TIMEOUT = 3000;

    @Override
    public MantaClient get() {
        ConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(),
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()),
                new StandardConfigContext().setExpectContinueTimeout(EXPECT_CONTINUE_DEFAULT_TIMEOUT));
        return new MantaClient(config);
    }
}
