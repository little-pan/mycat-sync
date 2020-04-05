package org.opencloudb.server.handler;

import org.opencloudb.BaseServerTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadDataInfileHandlerServerTest extends BaseServerTest {

    public static void main(String[] args) throws Exception {
        new LoadDataInfileHandlerServerTest().test();
    }

    @Override
    protected void doTest() throws Exception {
        testDefaultNodeTable("hotel", true, false, false);
        testDefaultNodeTable("hotel", false, false, false);

        // Tx test
        testDefaultNodeTable("hotel", true, true, false);
        testDefaultNodeTable("hotel", false, true, false);
        testDefaultNodeTable("hotel", true, true, true);
        testDefaultNodeTable("hotel", false, true, true);
    }

    private void testDefaultNodeTable(String table, boolean isLocal, boolean tx, boolean commit)
            throws SQLException {
        final int rows = 5;
        debug("testDefaultNodeTable: table '%s', local %s, tx %s, commit %s",
                table, isLocal, tx, commit);

        // Prepare
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            deleteTable(stmt, table);
        }

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            if (tx) c.setAutoCommit(false);

            String local = isLocal? "local": "";
            String file = getResFile("test.hotel.csv") + "";
            String sql = "load data %s infile '%s' into table %s(name, address, tel, rooms);";
            sql = format(sql, local, file, table);
            int n = stmt.executeUpdate(sql);
            assertTrue(n == rows, "'load data' affected rows error: " + n);
            if (tx) {
                if (commit) {
                    c.commit();
                } else {
                    c.rollback();
                }
            }
        }

        // Check again by query
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            int n = countTable(stmt, "hotel");
            if (!tx || commit) {
                assertTrue(n == rows, "'load data' rows error: " + n);
            } else {
                assertTrue(n == 0, "'load data' rows error after rollback: " + n);
            }
        }
    }

}
