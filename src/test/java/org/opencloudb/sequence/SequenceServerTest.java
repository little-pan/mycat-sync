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
import org.opencloudb.util.IoUtil;

import java.sql.*;
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

        testAutoIncrInsert(1, false, false);
        testAutoIncrInsert(2, false, false);
        testAutoIncrInsert(10, false, false);
        testAutoIncrInsert(1, true, false);
        testAutoIncrInsert(2, true, false);
        testAutoIncrInsert(10, true, false);
        testAutoIncrInsert(1, true, true);
        testAutoIncrInsert(2, true, true);
        testAutoIncrInsert(10, true, true);
        testAutoIncrBatchInsert(1, false, false);
        testAutoIncrBatchInsert(5, false, false);
        testAutoIncrBatchInsert(10, false, false);
        testAutoIncrBatchInsert(1, true, false);
        testAutoIncrBatchInsert(5, true, false);
        testAutoIncrBatchInsert(10, true, false);
        testAutoIncrBatchInsert(1, true, true);
        testAutoIncrBatchInsert(5, true, true);
        testAutoIncrBatchInsert(10, true, true);

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

        // Concurrent test
        testAutoIncrInsert(1, false, false, 2);
        testAutoIncrInsert(2, false, false, 5);
        testAutoIncrInsert(10, false, false, 9);
        testAutoIncrInsert(1, true, false, 2);
        testAutoIncrInsert(2, true, false, 5);
        testAutoIncrInsert(10, true, false, 9);
        testAutoIncrInsert(1, true, true, 2);
        testAutoIncrInsert(2, true, true, 5);
        testAutoIncrInsert(10, true, true, 9);
        testAutoIncrBatchInsert(1, false, false, 2);
        testAutoIncrBatchInsert(5, false, false, 5);
        testAutoIncrBatchInsert(10, false, false, 50);
        testAutoIncrBatchInsert(1, true, false, 2);
        testAutoIncrBatchInsert(5, true, false, 5);
        testAutoIncrBatchInsert(10, true, false, 50);
        testAutoIncrBatchInsert(1, true, true, 2);
        testAutoIncrBatchInsert(5, true, true, 5);
        testAutoIncrBatchInsert(10, true, true, 50);

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

    private void testAutoIncrInsert(int rows, boolean tx, boolean commit) throws SQLException {
        debug("testAutoIncrInsert: rows %d, tx %s, commit %s, single thread",
                rows, tx, commit);

        prepare();
        doTestAutoIncrInsert(rows, tx, commit, 1, true);
        checkTxResult(tx, commit, 1, rows);
    }

    private void testAutoIncrInsert(final int rows, final boolean tx, final boolean commit,
                                    final int threadCount) throws Exception {
        debug("testAutoIncrInsert: rows %d, tx %s, commit %s, threadCount %d",
                rows, tx, commit, threadCount);
        prepare();

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; ++i) {
            final long companyId = i + 1;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    doTestAutoIncrInsert(rows, tx, commit, companyId, threadCount == 1);
                }
            }, "testAutoIncrInsert-" + i);
            t.setDaemon(true);
            threads[i] = t;
            t.start();
        }

        for (Thread t: threads) {
            t.join();
        }

        checkTxResult(tx, commit, threadCount,rows * threadCount);
    }

    private void doTestAutoIncrInsert(int rows, boolean tx, boolean commit, long companyId, boolean noConcur) {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            int n;

            if (tx) c.setAutoCommit(false);
            insertCompany(stmt, companyId, "Company-"+companyId);
            final int beginRows = countTable(stmt, "employee");

            ResultSet rs = stmt.executeQuery("select next value for mycatseq_employee");
            assertTrue(rs.next(), "No sequence");
            final long lastId = rs.getLong(1);
            rs.close();

            // Do insert
            String sql = "insert into employee(company_id, empno, name)values(?, ?, ?)";
            PreparedStatement ps = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < rows; ++i) {
                ps.setLong(1, companyId);
                ps.setString(2, "00" + (i + 1));
                ps.setString(3, "员工-" + (i + 1));
                n = ps.executeUpdate();
                rs = ps.getGeneratedKeys();
                assertTrue(rs.next(), "No generated keys");
                if (noConcur) {
                    long id = rs.getLong(1);
                    assertTrue(lastId + i + 1 == id, "Auto increment ID error: " + id);
                }
                rs.close();
                ps.clearParameters();
                assertTrue(n == 1, "Update count error: " + n);
            }
            ps.close();

            if (tx) {
                n = countTable(stmt, "employee");
                assertTrue(n == beginRows + rows, "Insertion result error: result rows " + n);
            }

            if (tx) {
                if (commit) {
                    c.commit();
                } else {
                    c.rollback();
                }
            }
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected void prepare() {
        super.prepare();

        Connection c = null;
        try {
            Statement stmt;
            c = getConnection();

            stmt = c.createStatement();
            deleteTable(stmt, "employee");
            deleteTable(stmt, "company");

            stmt.close();
        } catch (SQLException e) {
            throw new AssertionError("Prepare fatal", e);
        } finally {
            IoUtil.close(c);
        }
    }

    private void checkTxResult(boolean tx, boolean commit, int comRows, int empRows) throws SQLException {
        debug("checkTxResult: tx %s, commit %s, comRows %d, empRows %d",
                tx, commit, comRows, empRows);
        if (!tx) {
            return;
        }

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            int n;
            if (commit) {
                n = countTable(stmt, "company");
                assertTrue(n == comRows, "Insertion commit error: company rows " + n);

                n = countTable(stmt, "employee");
                assertTrue(n == empRows, "Insertion commit error: employee rows " + n);
            } else {
                n = countTable(stmt, "company");
                assertTrue(n == 0, "Insertion rollback error: company rows " + n);

                n = countTable(stmt, "employee");
                assertTrue(n == 0, "Insertion rollback error: employee rows " + n);
            }
        }
    }

    private void testAutoIncrBatchInsert(int rows, boolean tx, boolean commit) throws SQLException {
        debug("testAutoIncrBatchInsert: rows %d, tx %s, commit %s, single thread",
                rows, tx, commit);

        prepare();
        doTestAutoIncrBatchInsert(rows, tx, commit, 1);
        checkTxResult(tx, commit, 1, rows);
    }

    private void testAutoIncrBatchInsert(final int rows, final boolean tx, final boolean commit, final int threadCount)
            throws Exception {
        debug("testAutoIncrBatchInsert: rows %d, tx %s, commit %s, threadCount %d",
                rows, tx, commit, threadCount);

        prepare();

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; ++i) {
            final long companyId = i + 1;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    doTestAutoIncrBatchInsert(rows, tx, commit, companyId);
                }
            }, "testAutoIncrBatchInsert-"+i);
            threads[i] = t;
            t.setDaemon(true);
            t.start();
        }

        for (Thread t: threads) {
            t.join();
        }

        checkTxResult(tx, commit, threadCount, rows * threadCount);
    }

    private void doTestAutoIncrBatchInsert(int rows, boolean tx, boolean commit, long companyId) {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet rs;

            if (tx) c.setAutoCommit(false);
            insertCompany(stmt, companyId, "c-"+companyId);
            final int beginRows = countTable(stmt, "employee");

            // Do insert
            String sql = "insert into employee(company_id, empno, name)values(?, ?, ?)";
            PreparedStatement ps = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < rows; ++i) {
                ps.setLong(1, companyId);
                ps.setString(2, "00" + (i + 1));
                ps.setString(3, "员工-" + (i + 1));
                ps.addBatch();
            }
            int[] a = ps.executeBatch();
            int n = a.length;
            assertTrue(n == rows, "Batch insertion result rows error: " + n);
            ps.close();

            rs = stmt.executeQuery("select count(*) from employee");
            assertTrue(rs.next(), "Batch insertion no data in table");
            if (tx) {
                n = rs.getInt(1);
                assertTrue(n == beginRows + rows, "Count rows error after batch insertion: " + n);
            }
            rs.close();

            if (tx) {
                if (commit) {
                    c.commit();
                } else {
                    c.rollback();
                }
            }
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
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
