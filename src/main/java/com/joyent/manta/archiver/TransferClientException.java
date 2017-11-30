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
 * Exception thrown when there is a problem communicating with the remote
 * data store.
 */
public class TransferClientException extends ContextedRuntimeException {
    private static final long serialVersionUID = -4798043699909928821L;

    public TransferClientException() {
    }

    public TransferClientException(final String message) {
        super(message);
    }

    public TransferClientException(final Throwable cause) {
        super(cause);
    }

    public TransferClientException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public TransferClientException(final String message, final Throwable cause, final ExceptionContext context) {
        super(message, cause, context);
    }
}
