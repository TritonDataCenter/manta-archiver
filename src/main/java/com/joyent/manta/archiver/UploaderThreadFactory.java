/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link java.util.concurrent.ThreadFactory} implementation that sets thread
 * properties for threads used to upload objects.
 */
class UploaderThreadFactory implements ThreadFactory {
    private static final ThreadGroup UPLOADER_THREAD_GROUP =
            new ThreadGroup("uploaders");
    private AtomicInteger threadCounter = new AtomicInteger();

    /**
     * Creates new instance.
     */
    UploaderThreadFactory() {
    }

    @Override
    public Thread newThread(final Runnable r) {
        final String name = String.format("uploader-thread-%d",
                threadCounter.incrementAndGet());
        final Thread uploaderThread = new Thread(UPLOADER_THREAD_GROUP, r, name);
        uploaderThread.setDaemon(true);
        // Prioritize CPU used for uploads
        uploaderThread.setPriority(Thread.MAX_PRIORITY);
        Thread.UncaughtExceptionHandler handler = new LoggingUncaughtExceptionHandler(
                "UploaderThreadPool");
        uploaderThread.setUncaughtExceptionHandler(handler);
        return uploaderThread;
    }
}
