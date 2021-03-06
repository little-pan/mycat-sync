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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

/** Csv file read/write util.
 *
 * @since 2020-04-06
 * @author little-pan
 */
public final class CsvUtil {

    public static File write(String table, List<?> rows) throws IOException {
        return write(null, false, table, rows, "\'", null, null);
    }

    public static File write(File csvFile, boolean append, String table, List<?> rows)
            throws IOException {
        return write(csvFile, append, table, rows, "\'", null, null);
    }

    public static File write(File csvFile, boolean append, String table, List<?> rows,
                             String enclose, String lineSeq, String charset) throws IOException {
        if (csvFile == null) {
            csvFile = File.createTempFile(table + "-", ".csv");
        }
        try (FileOutputStream out = new FileOutputStream(csvFile, append)) {
            write(out, rows, enclose, lineSeq, charset);
        }

        return csvFile;
    }

    public static void write(OutputStream out, List<?> rows) throws IOException {
        write(out, rows, "\'", null, null);
    }

    public static void write(OutputStream out, List<?> rows, String enclose,
                             String lineSeq, String charset) throws IOException {

        final byte[] lineSeqBytes;

        charset = charset == null? "UTF-8": charset;
        lineSeq = lineSeq == null? System.lineSeparator(): lineSeq;
        lineSeqBytes = lineSeq.getBytes(charset);

        for (Object row: rows) {
            int i = 0;
            for (Object val: (Collection<?>)row) {
                if (i > 0) {
                    out.write(',');
                }
                if (val != null) {
                    String s = val + "";
                    if (enclose != null) {
                        s = enclose + s + enclose;
                    }
                    byte[] a = s.getBytes(charset);
                    out.write(a);
                }
                ++i;
            }
            out.write(lineSeqBytes);
        }
    }

}
