/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Class containing methods used for compressing a file before it is sent to
 * Manta.
 */
public class ObjectCompressor {
    /**
     * File extension of compression algorithm used to compress objects.
     */
    public static final String COMPRESSION_TYPE = CompressorStreamFactory.XZ;

    private static final CompressorStreamFactory COMPRESSOR_STREAM_FACTORY =
            new CompressorStreamFactory();

    /**
     * Compresses stream using the XZ compression format.
     *
     * @param path path to the file being compressed (used in error messages)
     * @param out stream to compress
     * @return a stream wrapped in a XZ compression stream
     */
    OutputStream compress(final Path path, final OutputStream out) {
        try {
            return COMPRESSOR_STREAM_FACTORY.createCompressorOutputStream(COMPRESSION_TYPE, out);
        } catch (CompressorException e) {
            String msg = "Error compressing file";
            FileProcessingException fpe = new FileProcessingException(msg, e);
            fpe.setContextValue("path", path);

            throw fpe;
        }
    }

    /**
     * Compresses stream using the XZ compression format.
     *
     * @param mantaPath path in Manta to the file being decompressed (used in error messages)
     * @param in stream to decompress
     * @return a stream wrapped in a XZ decompression stream
     */
    InputStream decompress(final String mantaPath, final InputStream in) {
        try {
            return COMPRESSOR_STREAM_FACTORY.createCompressorInputStream(COMPRESSION_TYPE, in);
        } catch (CompressorException e) {
            String msg = "Error compressing file";
            FileProcessingException fpe = new FileProcessingException(msg, e);
            fpe.setContextValue("mantaPath", mantaPath);

            throw fpe;
        }
    }
}
