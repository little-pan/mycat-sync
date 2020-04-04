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
package org.opencloudb.sequence;

import org.opencloudb.BaseServerTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SequenceServerTest extends BaseServerTest {

    public static void main(String[] args) throws Exception {
        new SequenceServerTest().test();
    }

    @Override
    protected void doTest() throws Exception {
        testNextValue(2, false);
        testNextValue(3, false);
        testNextValue(100, false);
        testNextValue(102, false);
        testNextValue(10000, false);

        testNextValue(2, true);
        testNextValue(3, true);
        testNextValue(100, true);
        testNextValue(102, true);
        testNextValue(10000, true);

        ConcurrentMap<Long, Long> dupMap = new ConcurrentHashMap<>();
        testNextValue(2, dupMap, 2);
        testNextValue(10, dupMap, 2);
        testNextValue(100, dupMap, 9);
        testNextValue(102, dupMap, 10);
        testNextValue(201, dupMap, 20);
        testNextValue(100, dupMap, 100);
        testNextValue(150, dupMap, 250);
    }

    private void testNextValue(int n, final boolean tx) throws Exception {
        String sql = "select NEXT VALUE FOR MYCATSEQ_COMPANY";
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            long a = -1, b;
            for (int i = 0; i < n; ++i) {
                if (tx) c.setAutoCommit(false);
                ResultSet rs = stmt.executeQuery(sql);
                assertTrue(rs.next(), "No resultSet");
                b = rs.getLong(1);
                assertFalse(rs.next(), "More resultSet");
                rs.close();

                if (a != -1) {
                    assertTrue(a + 1 == b, "Next value error: a  = " + a + ", b = " + b);
                }
                a = b;
                if (tx) c.commit();
            }
        }
    }

    private void testNextValue(final int n, final ConcurrentMap<Long, Long> dupMap,
                               int threadCount) throws Exception {
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; ++i) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        testNextValue(n, dupMap);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }, "Thread-"+i);
            threads[i] = t;
            t.setDaemon(true);
            t.start();
        }

        for (Thread t: threads) {
            t.join();
        }
    }

    private void testNextValue(int n, ConcurrentMap<Long, Long> dupMap) throws Exception {
        String sql = "select NEXT VALUE FOR MYCATSEQ_COMPANY";
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            for (int i = 0; i < n; ++i) {
                ResultSet rs = stmt.executeQuery(sql);
                assertTrue(rs.next(), "No resultSet");
                long id = rs.getLong(1);
                assertFalse(rs.next(), "More resultSet");
                rs.close();

                assertNull(dupMap.putIfAbsent(id, id), "Duplicated seq: " + id);
            }
        }
    }

}
