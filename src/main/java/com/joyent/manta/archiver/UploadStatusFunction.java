/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import com.google.common.cache.Cache;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Function that outputs the detailed status of internal queues and thread pools.
 */
public class UploadStatusFunction implements Function<Void, Optional<RuntimeException>> {
    private final Future<TotalTransferDetails> transferDetailsFuture;
    private final AtomicLong totalUploads;
    private final AtomicLong noOfObjectToUpload;
    private final Cache<String, Boolean> dirCache;
    private final TransferQueue<ObjectUpload> uploadQueue;
    private final ForkJoinPool preloadForkJoinPool;

    @SuppressWarnings("JavadocMethod")
    public UploadStatusFunction(final Future<TotalTransferDetails> transferDetailsFuture,
                                final AtomicLong totalUploads,
                                final AtomicLong noOfObjectToUpload,
                                final Cache<String, Boolean> dirCache,
                                final TransferQueue<ObjectUpload> uploadQueue,
                                final ForkJoinPool preloadForkJoinPool) {
        this.transferDetailsFuture = transferDetailsFuture;
        this.totalUploads = totalUploads;
        this.noOfObjectToUpload = noOfObjectToUpload;
        this.dirCache = dirCache;
        this.uploadQueue = uploadQueue;
        this.preloadForkJoinPool = preloadForkJoinPool;
    }

    @Override
    public Optional<RuntimeException> apply(final Void aVoid) {
        try {
            printfln("Upload queue size: %d", uploadQueue.size());
            printfln("Total objects to upload [noOfObjectToUpload]: %d", noOfObjectToUpload.get());

            if (transferDetailsFuture.isDone()) {
                TotalTransferDetails transferDetails = getTotalTransferDetails();
                if (transferDetails != null) {
                    printfln("Total objects to upload: %d", transferDetails.numberOfObjects);
                    printfln("Total bytes to upload: %d", transferDetails.numberOfBytes);
                }
            } else {
                printfln("Transfer details is still being calculated...");
            }

            printfln("Total objects uploaded: %d", totalUploads.get());

            if (dirCache != null) {
                printfln("Directory cache size: %d", dirCache.size());
            }

            printfln("Preload ForkJoinPool active thread count: %d",
                    preloadForkJoinPool.getActiveThreadCount());
            printfln("Preload ForkJoinPool pool size: %d",
                    preloadForkJoinPool.getPoolSize());
            printfln("Preload ForkJoinPool running thread count: %d",
                    preloadForkJoinPool.getRunningThreadCount());
        } catch (RuntimeException e) {
            return Optional.of(e);
        }

        return Optional.empty();
    }

    private TotalTransferDetails getTotalTransferDetails() {
        try {
            return transferDetailsFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void printfln(final String pattern, final Object... params) {
        System.err.printf(pattern, params);
        System.err.println();
    }
}
