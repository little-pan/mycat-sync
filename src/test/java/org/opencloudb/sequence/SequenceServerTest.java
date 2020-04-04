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
import java.sql.SQLException;
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

        testNextValue(1, false, "GLOBAL");
        testNextValue(1, false, "global");
        testNextValue(1, false, "Global");

        // Test abnormal conditions
        try {
            testNextValue(1, false, "g");
        } catch (SQLException e) {
            debug("Test undefined sequence in conf: %s", e);
            assertTrue(1003 == e.getErrorCode(), e + "");
        }
        try {
            testNextValue(2, true, "hotel");
        } catch (SQLException e) {
            debug("Test undefined sequence in db: %s", e);
            assertTrue(1003 == e.getErrorCode(), e + "");
        }
        try {
            testNextValue(3, true, "customer");
        } catch (SQLException e) {
            debug("Test sequence increment illegal in db: %s", e);
            assertTrue(1003 == e.getErrorCode(), e + "");
        }

        ConcurrentMap<String, Long> dupMap = new ConcurrentHashMap<>();
        testNextValue(2, dupMap, 2);
        testNextValue(10, dupMap, 2);
        testNextValue(100, dupMap, 9);
        testNextValue(102, dupMap, 10);
        testNextValue(201, dupMap, 20);
        testNextValue(100, dupMap, 100);
        testNextValue(150, dupMap, 250);

        testMultiSeqNextValue(1, dupMap, 2, "GLOBAL", "company");
        testMultiSeqNextValue(2, dupMap, 5, "COMPANY", "global");
        testMultiSeqNextValue(10, dupMap, 10, "global", "COMPANY");
        testMultiSeqNextValue(20, dupMap, 50, "global", "company");
        testMultiSeqNextValue(100, dupMap, 100, "global", "company");
        testMultiSeqNextValue(100, dupMap, 250, "global", "company");
    }

    private void testNextValue(int n, final boolean tx) throws Exception {
        testNextValue(n, tx, "COMPANY");
    }

    private void testNextValue(int n, final boolean tx, String seqName) throws Exception {
        String sql = "select NEXT VALUE FOR MYCATSEQ_"+seqName;
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
                    assertTrue(a + 1 == b,
                            "Next value error: a  = " + a + ", b = " + b + " in seq " + seqName);
                }
                a = b;
                if (tx) c.commit();
            }
        }
    }

    private void testNextValue(final int n, final ConcurrentMap<String, Long> dupMap,
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

    private void testNextValue(int n, ConcurrentMap<String, Long> dupMap) throws Exception {
        testNextValue(n, dupMap, "COMPANY");
    }

    private void testMultiSeqNextValue(final int n, final ConcurrentMap<String, Long> dupMap,
                               int threadCount, String... seqList) throws Exception {
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; ++i) {
            final String seqName = seqList[i % seqList.length];
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        testNextValue(n, dupMap, seqName);
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

    private void testNextValue(int n, ConcurrentMap<String, Long> dupMap, String seqName) throws Exception {
        String sql = "select NEXT VALUE FOR MYCATSEQ_" + seqName;
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            for (int i = 0; i < n; ++i) {
                ResultSet rs = stmt.executeQuery(sql);
                assertTrue(rs.next(), "No resultSet");
                long id = rs.getLong(1);
                assertFalse(rs.next(), "More resultSet");
                rs.close();

                Long old = dupMap.putIfAbsent(seqName+id, id);
                assertNull(old, "Duplicated seq: " + seqName+id);
            }
        }
    }

}
