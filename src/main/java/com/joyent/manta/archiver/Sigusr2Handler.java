/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.Optional;
import java.util.function.Function;

/**
 * Flexible handler class for acting on SIGHUP signals.
 */
public class Siguser1Handler implements SignalHandler {
    private static final Logger LOG = LoggerFactory.getLogger(Siguser1Handler.class);

    private final Function<Void, Optional<RuntimeException>> function;

    /**
     * Create a new SIGHUP handler class.
     *
     * @param function function to execution upon SIGHUP
     */
    public Siguser1Handler(final Function<Void, Optional<RuntimeException>> function) {
        this.function = function;
    }

    @Override
    public void handle(final Signal signal) {
        LOG.debug("SIGUSR1 received");
        function.apply(null).ifPresent(e -> LOG.error("Error processing SIGHUP", e));
    }
}
