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
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
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
        ForkJoinPool executor = new ForkJoinPool();
        ObjectUploadQueueLoader loader = new ObjectUploadQueueLoader(executor);
        Path root = Paths.get("/opt/duck");

        ForkJoinTask<?> task = executor.submit(() -> loader.processDirectoryContents(root));

        try {
            while (!task.isDone()) {
                final ObjectUpload upload = loader.getQueue().poll(1, TimeUnit.SECONDS);

                if (upload == null) {
                    continue;
                }

                LOG.debug("Transferred from queue: {}", upload);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
        }
    }
}
