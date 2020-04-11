/*
 * Copyright (c) 2020, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/** An IO util.
 *
 * @author little-pan
 * @since 2020-03-21
 */
public final class IoUtil {

    static final Logger log = LoggerFactory.getLogger(IoUtil.class);

    static final String PROP_BUFFER_SIZE = "org.opencloudb.util.ioBufferSize";
    static final int BUFFER_SIZE = Integer.getInteger(PROP_BUFFER_SIZE, 4 << 10);

    private IoUtil() {}

    public static void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                log.debug("Close resource: {}", closeable);
                closeable.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    public static OutputStream fileOutputStream(File file) throws IOException {
        return fileOutputStream(file, false, true, 0);
    }

    public static OutputStream fileOutputStream(File file, int bufferSize) throws IOException {
        return fileOutputStream(file, false, true, bufferSize);
    }

    public static OutputStream fileOutputStream(File file, boolean append) throws IOException {
        return fileOutputStream(file, append, true, 0);
    }

    public static OutputStream fileOutputStream(File file, boolean append, int bufferSize) throws IOException {
        return fileOutputStream(file, append, true, bufferSize);
    }

    public static OutputStream fileOutputStream(File file, boolean append, boolean buffered)
            throws IOException {
        return fileOutputStream(file, append, buffered, 0);
    }

    public static OutputStream fileOutputStream(File file, boolean append, boolean buffered, int bufferSize)
            throws IOException {

        File parent = file.getParentFile();
        if (!parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("Create directory failed: " + parent);
        }

        OutputStream out = new FileOutputStream(file, append);
        if (buffered) {
            BufferedOutputStream bout;
            boolean failed = true;
            try {
                bufferSize = bufferSize <= 0? BUFFER_SIZE: bufferSize;
                bout = new BufferedOutputStream(out, bufferSize);
                failed = false;
            } finally {
                if (failed) {
                    close(out);
                }
            }
            return bout;
        } else {
            return out;
        }
    }

    public static InputStream fileInputStream(File file) throws IOException {
        return fileInputStream(file, true, 0);
    }

    public static InputStream fileInputStream(File file, int bufferSize) throws IOException {
        return fileInputStream(file, true, bufferSize);
    }

    public static InputStream fileInputStream(File file, boolean buffered) throws IOException {
        return fileInputStream(file, buffered, 0);
    }

    public static InputStream fileInputStream(File file, boolean buffered, int bufferSize)
            throws IOException {

        File parent = file.getParentFile();
        if (!parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("Create directory failed: " + parent);
        }

        InputStream in = new FileInputStream(file);
        if (buffered) {
            BufferedInputStream bin;
            boolean failed = true;
            try {
                bufferSize = bufferSize <= 0? BUFFER_SIZE: bufferSize;
                bin = new BufferedInputStream(in, bufferSize);
                failed = false;
            } finally {
                if (failed) {
                    IoUtil.close(in);
                }
            }
            return bin;
        } else {
            return in;
        }
    }

}
