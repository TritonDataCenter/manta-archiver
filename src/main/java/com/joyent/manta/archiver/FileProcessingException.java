/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.apache.commons.lang3.exception.ContextedRuntimeException;
import org.apache.commons.lang3.exception.ExceptionContext;

/**
 * Exception class for errors that occur when processing a file and passing it
 * off to Manta.
 */
public class FileProcessingException extends ContextedRuntimeException {
    private static final long serialVersionUID = 4660932764637591269L;

    public FileProcessingException() {
    }

    public FileProcessingException(final String message) {
        super(message);
    }

    public FileProcessingException(final Throwable cause) {
        super(cause);
    }

    public FileProcessingException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public FileProcessingException(final String message, final Throwable cause,
                                   final ExceptionContext context) {
        super(message, cause, context);
    }
}
