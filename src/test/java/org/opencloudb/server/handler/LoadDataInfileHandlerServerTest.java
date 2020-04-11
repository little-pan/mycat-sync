package org.opencloudb.server.handler;

import org.opencloudb.BaseServerTest;
import org.opencloudb.util.CsvUtil;
import org.opencloudb.util.IoUtil;

import java.io.File;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class LoadDataInfileHandlerServerTest extends BaseServerTest {

    public static void main(String[] args) throws Exception {
        new LoadDataInfileHandlerServerTest().test();
    }

    public LoadDataInfileHandlerServerTest() {

    }

    public LoadDataInfileHandlerServerTest(int rounds) {
        super(rounds);
    }

    @Override
    protected void doTest() throws Exception {
        testDefaultTable(true, false, false);
        testDefaultTable(false, false, false);
        testGlobalTable(true, false, false);
        testGlobalTable(false, false, false);
        testShardTable(true, false, false, false, false);
        testShardTable(false, false, false, false, false);
        testShardTable(true, false, false, true, false);
        testShardTable(false, false, false, true, false);
        testShardTable(true, false, false, true, true);
        testShardTable(false, false, false, false, true);

        // Tx test
        testDefaultTable(true, true, false);
        testDefaultTable(false, true, false);
        testDefaultTable(true, true, true);
        testDefaultTable(false, true, true);
        testGlobalTable(true, true, false);
        testGlobalTable(false, true, false);
        testGlobalTable(true, true, true);
        testGlobalTable(false, true, true);
        testShardTable(true, true, true, true, false);
        testShardTable(true, true, true, false, false);
        testShardTable(true, true, false, true, false);
        testShardTable(true, true, false, false, false);
        testShardTable(false, true, true, true, false);
        testShardTable(false, true, true, false, false);
        testShardTable(false, true, false, true, false);
        testShardTable(false, true, false, false, false);
        testShardTable(false, true, false, true, true);
        testShardTable(false, true, false, false, true);

        // Perf test: 200k rows/s
        // 5s
        testDefaultTablePerf(true, 1000000);
        testDefaultTablePerf(false, 1000000);
        // 15s: 3 nodes
        testGlobalTablePerf(true, 1000000);
        testGlobalTablePerf(false, 1000000);
        // 10s: 2 nodes, ID assigned
        testShardTablePerf(true, 1000000, false, false);
        // *ID generator of mycat db sequence*
        // 170s: 2 nodes, ID auto increment(step  20)
        //  80s: 2 nodes, ID auto increment(step  50)
        //  46s: 2 nodes, ID auto increment(step 100)
        testShardTablePerf(true, 1000000, true, true);
        //  48s
        testShardTablePerf(true, 1000000, true, false);
        //  45s
        testShardTablePerf(false, 1000000, true, true);
        //  47s
        testShardTablePerf(false, 1000000, true, false);
        //  10s
        testShardTablePerf(false, 1000000, false, false);
        // 25s
        testDefaultTablePerf(true, 5000000);
        testDefaultTablePerf(false, 5000000);
        testGlobalTablePerf(true, 5000000);
        testGlobalTablePerf(false, 5000000);
        // 50s
        testDefaultTablePerf(true, 10000000);
        testDefaultTablePerf(false, 10000000);
        testGlobalTablePerf(true, 10000000);
        testGlobalTablePerf(false, 10000000);
    }

    private void testDefaultTablePerf(boolean isLocal, int rows) throws Exception {
        if (!TEST_PERF || !TEST_DTBL) {
            return;
        }

        String tag = "testDefaultTablePerf";
        String table = "hotel";
        String[] columns = {"name", "address", "tel", "rooms"};
        RowGenerator generator = new RowGenerator() {
            @Override
            public void generate(List<Object> row, int rowid, DateFormat dateFormat) {
                row.add("Hotel-" + rowid);
                row.add("Address-" + rowid);
                row.add(10000000000L + rowid);
                row.add(rowid % 10000);
            }
        };

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            dropTable(stmt, table);
            createTableHotel(stmt);
        }
        testPerf(tag, table, columns, generator, isLocal, rows, null, false);
    }

    private void testGlobalTablePerf(boolean isLocal, int rows) throws Exception {
        if (!TEST_PERF || !TEST_GTBL) {
            return;
        }

        String tag = "testGlobalTablePerf";
        String table = "company";
        String[] columns = {"id", "name", "address", "create_date"};
        RowGenerator generator = new RowGenerator() {
            @Override
            public void generate(List<Object> row, int rowid, DateFormat dateFormat) {
                row.add(rowid);
                row.add("Company-" + rowid);
                row.add("Address-" + rowid);
                row.add(dateFormat.format(new Date()));
            }
        };

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            dropTable(stmt, "employee");
            dropTable(stmt, table);
            createTableCompany(stmt);
            createTableEmployee(stmt);
        }
        testPerf(tag, table, columns, generator, isLocal, rows, null, true);
    }

    private void testShardTablePerf(boolean isLocal, int rows, final boolean autoIncr,
                                    final boolean noIdColumn) throws Exception {

        if (!TEST_PERF || !TEST_STBL) {
            return;
        }

        String fileTag = (autoIncr? "id-autoIncr": "id-assigned");
        fileTag += (noIdColumn? "-noIdColumn": "-incIdColumn");

        String tag = "testShardTablePerf." + fileTag;
        String table = "employee";
        String[] columns;
        if (noIdColumn) {
            columns = new String[]{"company_id", "empno", "name"};
        } else {
            columns = new String[]{"id", "company_id", "empno", "name"};
        }
        final long companyA = 1L, companyB = 2L;
        boolean assignedId = !noIdColumn && !autoIncr;
        RowGenerator generator = new RowGenerator() {
            @Override
            public void generate(List<Object> row, int rowid, DateFormat dateFormat) {
                if (!noIdColumn) {
                    row.add(autoIncr? null: rowid);
                }
                row.add(rowid % 2 == 0? companyA: companyB);
                row.add("00" + rowid);
                row.add("Employee-"+ rowid % 1000);
            }
        };

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            dropTable(stmt, table);
            dropTable(stmt, "company");
            createTableCompany(stmt);
            createTableEmployee(stmt);
        }

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            insertCompany(stmt, companyA, "Company-" + companyA);
            insertCompany(stmt, companyB, "Company-" + companyB);
        }

        testPerf(tag, table, columns, generator, isLocal, rows, fileTag, assignedId);
    }

    private void testPerf(String tag, String table, String[] columns, RowGenerator rowGenerator,
                          boolean isLocal, int rows, String fileTag, boolean assignedId) throws Exception {
        debug(tag+": table '%s', local %s, rows %s", table, isLocal, rows);

        final int cols = columns.length;
        if (cols == 0) {
            throw new IllegalArgumentException("columns count: " + cols);
        }
        String name = table + "-perf-" + rows + (fileTag == null? "": "-"+fileTag) + ".csv";
        File csvFile = new File(DATA_DIR, name);
        if (!csvFile.isFile()) {
            info("'%s' not exists, create it", name);
            DateFormat df = getDateFormat();
            int batchSize = 10000;
            List<List<?>> batch = new ArrayList<>(batchSize);
            try (OutputStream out = IoUtil.fileOutputStream(csvFile, true)) {
                for (int i = 0; i < rows; ++i) {
                    List<Object> row = new ArrayList<>(cols);
                    rowGenerator.generate(row, i + 1, df);
                    batch.add(row);

                    if ((i + 1) % batchSize == 0 || i == rows -1) {
                        CsvUtil.write(out, batch);
                        batch.clear();
                    }
                }
            }

            info("'%s' created", name);
        }

        info(tag+": 'load data' start");
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            String local = isLocal? "local": "";
            String file = csvFile + "";
            String fields = "";
            for (int i = 0; i < cols; ++i) {
                if (i > 0) {
                    fields += ",";
                }
                fields += columns[i];
            }
            String sql = "load data %s infile '%s' into table %s " +
                    "fields terminated by ',' enclosed by '\\'' " +
                    "(" + fields + ")";
            sql = format(sql, local, file, table);
            int n = stmt.executeUpdate(sql);
            assertTrue(n == rows, "'load data' result rows: " + n);

            if (assignedId) {
                n = 3;
                ResultSet rs = stmt.executeQuery("select " + fields + " from " + table
                        + " order by " + columns[0] + " asc limit " + n);
                for (int i = 0; i < n; ++i) {
                    assertTrue(rs.next());
                    long id = rs.getLong(1);
                    long expected = i + 1;
                    assertEquals(expected, id, "ID not assigned");
                }
            }
        }
        info(tag+": 'load data' end");
    }

    private void testDefaultTable(boolean isLocal, boolean tx, boolean commit)
            throws Exception {
        if (!TEST_DTBL) {
            return;
        }

        String table = "hotel";
        debug("testDefaultTable: table '%s', local %s, tx %s, commit %s",
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

    private void testGlobalTable(boolean isLocal, boolean tx, boolean commit)
            throws Exception {
        if (!TEST_GTBL) {
            return;
        }

        String table = "company";
        debug("testGlobalTable: table '%s', local %s, tx %s, commit %s",
                table, isLocal, tx, commit);

        List<?> companies = Arrays.asList(
                Arrays.asList(1L, "Company-1", "Address-1", "2020-04-06"),
                Arrays.asList(2L, "Company-2", "Address-2", "2020-04-06"),
                Arrays.asList(3L, "Company-3", "Address-3", "2020-04-06"),
                Arrays.asList(4L, "Company-4", "Address-4", "2020-04-06"),
                Arrays.asList(5L, "Company-5", "Address-5", "2020-04-06"));
        final int rows = companies.size();
        File csvFile = CsvUtil.write(table, companies);

        prepare();

        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            if (tx) c.setAutoCommit(false);

            String local = isLocal? "local": "";
            String file = csvFile + "";
            String sql = "load data %s infile '%s' into table %s " +
                    "fields terminated by ',' enclosed by '\\'' " +
                    "(id, name, address, create_date)";
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
            DateFormat df = getDateFormat();
            int n = countTable(stmt, table);
            if (!tx || commit) {
                String sql;
                assertTrue(n == rows, "'load data' rows error: " + n);
                sql = "select id, name, address, create_date from " + table;
                ResultSet rs  = stmt.executeQuery(sql);
                for (int i = 0; i < n; ++i) {
                    assertTrue(rs.next());

                    long id = rs.getLong(1);
                    String name = rs.getString(2);
                    String address = rs.getString(3);
                    Date createDate = rs.getDate(4);

                    List<?> row = (List<?>)companies.get(i);
                    assertEquals(row.get(0), id);
                    assertEquals(row.get(1), name);
                    assertEquals(row.get(2), address);
                    assertEquals(row.get(3), df.format(createDate));
                }
            } else {
                assertTrue(n == 0, "'load data' rows error after rollback: " + n);
            }
        }
    }

    private void testShardTable(boolean isLocal, boolean tx, boolean commit, boolean autoIncr, boolean noIdColumn)
            throws Exception {
        if (!TEST_STBL) {
            return;
        }

        String table = "employee";
        debug("testShardTable: table '%s', local %s, tx %s, commit %s, autoIncr %s, noIdColumn %s",
                table, isLocal, tx, commit, autoIncr, noIdColumn);

        prepare();

        long companyA = 1L, companyB = 2L;
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            insertCompany(stmt, companyA, "Company-" + companyA);
            insertCompany(stmt, companyB, "Company-" + companyB);
        }

        final List<?> rows;
        if (noIdColumn) {
            rows = Arrays.asList(
                    // company_id, empno, name
                    Arrays.asList(companyA, "001", "Employee-" + 1),
                    Arrays.asList(companyA, "002", "Employee-" + 2),
                    Arrays.asList(companyB, "001", "Employee-" + 1),
                    Arrays.asList(companyB, "002", "Employee-" + 2),
                    Arrays.asList(companyA, "003", "Employee-" + 3));
        } else {
            rows = Arrays.asList(
                    // id, company_id, empno, name
                    Arrays.asList(autoIncr? null: 1L, companyA, "001", "Employee-" + 1),
                    Arrays.asList(autoIncr? null: 2L, companyA, "002", "Employee-" + 2),
                    Arrays.asList(autoIncr? null: 3L, companyB, "001", "Employee-" + 1),
                    Arrays.asList(autoIncr? null: 4L, companyB, "002", "Employee-" + 2),
                    Arrays.asList(autoIncr? null: 5L, companyA, "003", "Employee-" + 3));
        }

        final int rowCount = rows.size();
        File csvFile = CsvUtil.write(table, rows);



        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            if (tx) c.setAutoCommit(false);

            String local = isLocal? "local": "";
            String file = csvFile + "";
            String sql = "load data %s infile '%s' into table %s " +
                    "fields terminated by ',' enclosed by '\\'' " +
                    "(" + (noIdColumn? "": "id, ") + "company_id, empno, name)";
            sql = format(sql, local, file, table);
            int n = stmt.executeUpdate(sql);
            assertEquals(rowCount, n);
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
                assertTrue(n == rowCount, "'load data' rows error: " + n);
                sql = "select id, company_id, empno, name from " + table + " order by id asc";
                ResultSet rs  = stmt.executeQuery(sql);
                for (int i = 0; i < n; ++i) {
                    assertTrue(rs.next());

                    long id = rs.getLong(1);
                    boolean idNotNull = !rs.wasNull();
                    long companyId = rs.getLong(2);
                    String empno = rs.getString(3);
                    String name = rs.getString(4);

                    List<?> row = (List<?>)rows.get(i);
                    assertTrue(idNotNull);
                    int j = 0;
                    if (!autoIncr && !noIdColumn) {
                        assertEquals(row.get(j++), id);
                    }
                    if (autoIncr && !noIdColumn) {
                        j++;
                    }
                    assertEquals(row.get(j++), companyId);
                    assertEquals(row.get(j++), empno);
                    assertEquals(row.get(j), name);
                }
            } else {
                assertTrue(n == 0, "'load data' rows error after rollback: " + n);
            }
        }
    }

    interface RowGenerator {

        void generate(List<Object> row, int rowid, DateFormat dateFormat);

    }

}
