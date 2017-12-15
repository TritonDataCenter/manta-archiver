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
class NamedThreadFactory implements ThreadFactory {
    private AtomicInteger threadCounter = new AtomicInteger();

    private final String threadFormat;
    private final String loggerName;
    private final ThreadGroup threadGroup;

    /**
     * Creates new instance.
     * @param threadFormat format with %d parameter for naming new threads
     * @param threadGroupName name of thread group to put threads in
     * @param loggerName name of logger to log exceptions to
     */
    NamedThreadFactory(final String threadFormat, final String threadGroupName,
                       final String loggerName) {
        this.threadFormat = threadFormat;
        this.loggerName = loggerName;
        this.threadGroup = new ThreadGroup(threadGroupName);
    }

    @Override
    public Thread newThread(final Runnable r) {
        final String name = String.format(threadFormat,
                threadCounter.incrementAndGet());
        final Thread uploaderThread = new Thread(threadGroup, r, name);
        uploaderThread.setDaemon(true);
        // Prioritize CPU used for uploads
        uploaderThread.setPriority(Thread.MAX_PRIORITY);
        Thread.UncaughtExceptionHandler handler = new LoggingUncaughtExceptionHandler(
                loggerName);
        uploaderThread.setUncaughtExceptionHandler(handler);
        return uploaderThread;
    }
}
