/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception handler class that will log exceptions to a configurable logger.
 */
public class LoggingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private final Logger logger;

    /**
     * Creates a new instance that logs to the specified logger.
     * @param loggerName logger name to log to
     */
    public LoggingUncaughtExceptionHandler(final String loggerName) {
        this.logger = LoggerFactory.getLogger(loggerName);
    }

    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        logger.error(rootCause.getMessage(), e);
    }
}
