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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * {@link Callable} implementation that handles file uploads.
 */
class ObjectUploadCallable implements Callable<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectUploadCallable.class);

    private final AtomicLong totalUploads;
    private final CountDownLatch latch;
    private final TransferQueue<ObjectUpload> queue;
    private final long noOfObjectToUpload;
    private final Consumer<ObjectUpload> uploadMethod;

    public ObjectUploadCallable(final AtomicLong totalUploads, final CountDownLatch latch, final TransferQueue<ObjectUpload> queue, final long noOfObjectToUpload, final Consumer<ObjectUpload> uploadMethod) {
        this.totalUploads = totalUploads;
        this.latch = latch;
        this.queue = queue;
        this.noOfObjectToUpload = noOfObjectToUpload;
        this.uploadMethod = uploadMethod;
    }

    @Override
    public Void call() throws Exception {
        try {
            while (totalUploads.get() < noOfObjectToUpload) {
                ObjectUpload upload = queue.poll(1, TimeUnit.SECONDS);

                if (upload == null) {
                    continue;
                }

                try {
                    uploadMethod.accept(upload);
                } catch (RuntimeException e) {
                    LOG.error("Error uploading file. Adding file back to the queue");
                    queue.put(upload);
                    continue;
                }

                /* After an upload is successful we want to know if we
                 * have uploaded all of the objects that we can upload
                 * because this will let us know when to exit the loop. */
                if (totalUploads.incrementAndGet() == noOfObjectToUpload) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        latch.countDown();
        return null;
    }
}
