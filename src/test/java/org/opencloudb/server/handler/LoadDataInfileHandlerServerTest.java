package org.opencloudb.server.handler;

import org.opencloudb.BaseServerTest;
import org.opencloudb.util.CsvUtil;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoadDataInfileHandlerServerTest extends BaseServerTest {

    public static void main(String[] args) throws Exception {
        new LoadDataInfileHandlerServerTest().test();
    }

    @Override
    protected void doTest() throws Exception {
        testDefaultNodeTable(true, false, false);
        testDefaultNodeTable(false, false, false);

        // Tx test
        testDefaultNodeTable(true, true, false);
        testDefaultNodeTable(false, true, false);
        testDefaultNodeTable(true, true, true);
        testDefaultNodeTable(false, true, true);

        // Perf test: 200k rows/s
        if (TEST_PERF) {
            // 5s
            testDefaultNodeTablePerf(true, 1000000);
            testDefaultNodeTablePerf(false, 1000000);
            // 25s
            testDefaultNodeTablePerf(true, 5000000);
            testDefaultNodeTablePerf(false, 5000000);
            // 50s
            testDefaultNodeTablePerf(true, 10000000);
            testDefaultNodeTablePerf(false, 10000000);
        }
    }

    private void testDefaultNodeTablePerf(boolean isLocal, int rows) throws Exception {
        String table = "hotel";
        debug("testDefaultNodeTablePerf: table '%s', local %s, rows %s",
                table, isLocal, rows);

        prepare();

        String name = table + "-perf-" + rows + ".csv";
        File csvFile = new File(DATA_DIR, name);
        if (!csvFile.isFile()) {
            info("'%s' not exists, create it", name);
            int batchSize = 10000;
            List<List<?>> hotels = new ArrayList<>(batchSize);
            for (int i = 0; i < rows; ++i) {
                List<Object> hotel = new ArrayList<>(4);
                hotel.add("Hotel-" + i);
                hotel.add("Address-" + i);
                hotel.add(10000000000L + i);
                hotel.add(i % batchSize);
                hotels.add(hotel);

                if ((i + 1) % batchSize == 0 || i == rows -1) {
                    CsvUtil.write(csvFile, true, table, hotels);
                    hotels.clear();
                }
            }
            info("'%s' created", name);
        }

        info("testDefaultNodeTablePerf: 'load data' start");
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            String local = isLocal? "local": "";
            String file = csvFile + "";
            String sql = "load data %s infile '%s' into table %s " +
                    "fields terminated by ',' enclosed by '\\'' " +
                    "(name, address, tel, rooms)";
            sql = format(sql, local, file, table);
            int n = stmt.executeUpdate(sql);
            assertTrue(n == rows, "'load data' result rows: " + n);
        }
        info("testDefaultNodeTablePerf: 'load data' end");
    }

    private void testDefaultNodeTable(boolean isLocal, boolean tx, boolean commit)
            throws Exception {
        String table = "hotel";
        debug("testDefaultNodeTable: table '%s', local %s, tx %s, commit %s",
                table, isLocal, tx, commit);

        List<?> hotels = Arrays.asList(
                Arrays.asList("Hotel-1", "Address-1", "11111111111", 10),
                Arrays.asList("Hotel-2", "酒店地址-2", "22222222222", 20),
                Arrays.asList("酒店名-3", "Address-3", "33333333333", 30),
                Arrays.asList("Hotel-4", "Address-4", "44444444444", 40),
                Arrays.asList("Hotel-5", "Address-5", "55555555555", 50));
        final int rows = hotels.size();
        File csvFile = CsvUtil.write(table, hotels);

        prepare();

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            if (tx) c.setAutoCommit(false);

            String local = isLocal? "local": "";
            String file = csvFile + "";
            String sql = "load data %s infile '%s' into table %s " +
                    "fields terminated by ',' enclosed by '\\'' " +
                    "(name, address, tel, rooms)";
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
            int n = countTable(stmt, table);
            if (!tx || commit) {
                String sql;
                assertTrue(n == rows, "'load data' rows error: " + n);
                sql = "select name, address, tel, rooms from " + table;
                ResultSet rs  = stmt.executeQuery(sql);
                for (int i = 0; i < n; ++i) {
                    assertTrue(rs.next());

                    String name = rs.getString(1);
                    String address = rs.getString(2);
                    String tel = rs.getString(3);
                    int roms = rs.getInt(4);

                    List<?> row = (List<?>)hotels.get(i);
                    assertEquals(row.get(0), name);
                    assertEquals(row.get(1), address);
                    assertEquals(row.get(2), tel);
                    assertEquals(row.get(3), roms);
                }
            } else {
                assertTrue(n == 0, "'load data' rows error after rollback: " + n);
            }
        }
    }

    @Override
    protected void prepare() {
        super.prepare();

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            truncateTable(stmt, "hotel");
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

}
