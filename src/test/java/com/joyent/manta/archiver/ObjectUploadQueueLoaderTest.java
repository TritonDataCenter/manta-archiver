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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

@Test
public class ObjectUploadQueueLoaderTest {
    private static Logger LOG = LoggerFactory.getLogger(ObjectUploadQueueLoaderTest.class);

    public void canAppendPaths() {
        Path parent = Paths.get("/var/quackery");
        Path subPath = Paths.get("/opt/duck");

        Path expected = Paths.get("/var/quackery/opt/duck");
        Path actual = ObjectUploadQueueLoader.appendPaths(parent, subPath);

        assertEquals(actual.toString(), expected.toString());
    }

    public void canProcessDirectory() {
        ForkJoinPool executor = new ForkJoinPool(ForkJoinPool.getCommonPoolParallelism(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                new LoggingUncaughtExceptionHandler("UnitTestForkJoinPool"),
                true);
        ObjectUploadQueueLoader loader = new ObjectUploadQueueLoader(executor,10);
        Path root = Paths.get("/opt/duck");

        TotalTransferDetails transferDetails = loader.uploadDirectoryContents(root);
        long numberTransferred = 0L;
        long totalBytesTransferred = 0L;

        try {
            while (numberTransferred < transferDetails.numberOfObjects) {
                final ObjectUpload upload = loader.getQueue().poll(1, TimeUnit.SECONDS);

                if (upload != null) {
                    numberTransferred++;
                    if (!upload.isDirectory()) {
                        totalBytesTransferred += upload.getSourcePath().toFile().length();
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
        }

        Assert.assertEquals(numberTransferred, transferDetails.numberOfObjects,
                "Number of actual files transferred doesn't equal the number enqueued");
        Assert.assertEquals(totalBytesTransferred, transferDetails.numberOfBytes,
                "Number of bytes transferred doesn't equal the number enqueued");
    }
}
