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
package org.opencloudb;

import java.io.File;
import java.io.PrintStream;
import java.sql.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public abstract class BaseServerTest {

    protected static final String USER_DIR = System.getProperty("user.dir");
    protected static final String RES_DIR = new File(USER_DIR, "conf") + "";
    protected static final String TEMP_DIR = new File(USER_DIR, "temp") + "";
    protected static final String DATA_DIR = new File(TEMP_DIR, "data") + "";

    static final int DEBUG = 1, INFO = 2, ERROR = 3;
    static final int LOG_LEVEL = Integer.getInteger("org.opencloudb.test.logLevel", DEBUG);
    static final int ROUNDS = Integer.getInteger("org.opencloudb.test.rounds", 2);
    protected static final boolean TEST_PERF = Boolean.getBoolean("org.opencloudb.test.perf");

    protected static String JDBC_USER = "root";
    protected static String JDBC_PASSWORD = "123456";
    protected static String JDBC_URL = "jdbc:mysql://localhost:8066/test?" +
            "useUnicode=true&characterEncoding=UTF-8&" +
            "connectTimeout=3000&socketTimeout="+ (TEST_PERF? 300000: 30000);

    static {
        // First boot MyCat server
        trySetProperty("MYCAT_HOME", USER_DIR);
        trySetProperty("org.opencloudb.server.daemon", "true");
        String[] args = new String[]{};
        MycatStartup.main(args);
    }

    public void test() throws Exception {
        for (int i = 0; i < ROUNDS; ++i) {
            prepare();
            try {
                final long a = System.currentTimeMillis();
                String testCase = getClass().getSimpleName();
                info("r-%d >> %s", i, testCase);
                doTest();
                final long b = System.currentTimeMillis();
                info("r-%d << %s: time %sms", i, testCase, b - a);
            } finally {
                cleanup();
            }
        }
    }

    protected void prepare() {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();

            dropTable(stmt, "hotel");
            dropTable(stmt, "employee");
            dropTable(stmt, "company");

            createTableCompany(stmt);
            createTableEmployee(stmt);
            createTableHotel(stmt);
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    protected void cleanup() {}

    protected abstract void doTest() throws Exception;

    protected static File getResFile(String filename) {
        return new File(RES_DIR, filename);
    }

    protected static Connection getConnection() {
        try {
            return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
        } catch (SQLException e) {
            throw new AssertionError("Can't get jdbc connection", e);
        }
    }

    protected static int createTableHotel(Statement stmt) throws SQLException {
        String sql = "create table if not exists hotel (" +
                "    id bigint not null auto_increment," +
                "    name varchar(20) not null," +
                "    address varchar(250)," +
                "    tel varchar(20)," +
                "    rooms int default 50 not null," +
                "    primary key(id)" +
                ")";
        return stmt.executeUpdate(sql);
    }

    protected static int createTableCompany(Statement stmt) throws SQLException {
        String sql = "create table if not exists company (" +
                "    id bigint not null primary key," +
                "    name varchar(20) not null," +
                "    address varchar(250)," +
                "    create_date date not null," +
                "    unique u_company_name(name)" +
                ")";
        return stmt.executeUpdate(sql);
    }

    protected static int createTableEmployee(Statement stmt) throws SQLException {
        String sql = "create table if not exists employee (" +
                "    id bigint not null primary key auto_increment," +
                "    company_id bigint not null," +
                "    empno varchar(10) not null," +
                "    name varchar(50) not null," +
                "    salary integer default 5000 not null," +
                "    gender char(1) default 'M' not null," +
                "    entry_date date," +
                "    leave_date date," +
                "    unique u_employee_empno(company_id, empno)," +
                "    foreign key(company_id) references company(id)" +
                ")";
        return stmt.executeUpdate(sql);
    }

    protected static int dropTable(Statement stmt, String table) throws SQLException {
        String sql = "drop table if exists " + table;
        return stmt.executeUpdate(sql);
    }


    protected static int deleteTable(Statement stmt, String table) throws SQLException {
        return stmt.executeUpdate("delete from " + table);
    }

    protected static int truncateTable(Statement stmt, String table) throws SQLException {
        return stmt.executeUpdate("truncate table " + table);
    }

    protected static int countTable(Statement stmt, String table, String where) throws SQLException {
        if (where == null) {
            where = "";
        }
        ResultSet rs = stmt.executeQuery("select count(*) from " + table + " " + where);
        rs.next();
        int n = rs.getInt(1);
        rs.close();
        return n;
    }

    protected static int countTable(Statement stmt, String table) throws SQLException {
        return countTable(stmt, table, null);
    }

    protected static int insertCompany(Statement stmt, long id, String name) throws SQLException {
        if (name == null) {
            throw new NullPointerException("name is null");
        }

        DateFormat df = getDateFormat();
        String sql = format("insert into company(id, name, create_date)values(%d, '%s', '%s')",
                id, name, df.format(new Date()));
        return stmt.executeUpdate(sql);
    }

    protected static DateFormat getDateFormat() {
        return new SimpleDateFormat("yyyy-MM-dd");
    }

    protected static DateFormat getTimeFormat() {
        return new SimpleDateFormat("HH:mm:ss");
    }

    protected static DateFormat getTimestampFormat() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    public static String format(String format, Object... args) {
        return String.format(format, args);
    }

    public static void info(String format, Object... args) {
        if (INFO >= LOG_LEVEL) {
            log(System.out, "[INFO ]", format, args);
        }
    }

    public static void debug(String format, Object... args) {
        if (DEBUG  >= LOG_LEVEL) {
            log(System.out, "[DEBUG]", format, args);
        }
    }

    public static void error(String format, Object... args) {
        if (ERROR >= LOG_LEVEL) {
            log(System.err, "[ERROR]", format, args);
        }
    }

    protected static void log(PrintStream out, String level, String format, Object... args) {
        DateFormat df = new SimpleDateFormat("HH:mm:ss.SSS");
        String threadName = Thread.currentThread().getName();
        String time = df.format(new Date());
        String message = String.format(format, args);
        // time + level + thread-name + message
        message = String.format("%s%s[%s] %s", time, level, threadName, message);
        out.println(message);
    }

    public static void trySetProperty(String name, String value) {
        if (System.getProperty(name) == null) {
            System.setProperty(name, value);
        }
    }

    public static void assertTrue(boolean b, String message) {
        if (!b) {
            throw new AssertionError(message);
        }
    }

    public static void assertTrue(boolean b) {
        if (!b) {
            throw new AssertionError("Expect true, but false");
        }
    }

    public static void assertFalse(boolean b, String message) {
        if (b) {
            throw new AssertionError(message);
        }
    }

    public static void assertFalse(boolean b) {
        if (b) {
            throw new AssertionError("Expect false, but true");
        }
    }

    public static void assertEquals(Object a, Object b, String message) {
        if (a == b) {
            return;
        }
        if (a == null || !a.equals(b)) {
            throw new AssertionError(message);
        }
    }

    public static void assertEquals(Object a, Object b) {
        if (a == b) {
            return;
        }
        if (a == null || !a.equals(b)) {
            throw new AssertionError("Expect " + a + ", but " + b);
        }
    }

    public static void assertNull(Object a) {
        if (a != null) {
            throw new AssertionError("Not null: " + a);
        }
    }

    public static void assertNull(Object a, String message) {
        if (a != null) {
            throw new AssertionError(message);
        }
    }

    public static void assertNotNull(Object a) {
        if (a == null) {
            throw new AssertionError("Null");
        }
    }

    public static void assertNotNull(Object a, String message) {
        if (a == null) {
            throw new AssertionError(message);
        }
    }

    public static void fail(String message) {
        throw new AssertionError(message);
    }

    public static void fail() {
        throw new AssertionError();
    }

}
