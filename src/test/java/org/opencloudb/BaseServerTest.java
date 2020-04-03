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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class BaseServerTest {

    protected static final String USER_DIR = System.getProperty("user.dir");

    protected static String JDBC_USER = "root";
    protected static String JDBC_PASSWORD = "123456";
    protected static String JDBC_URL = "jdbc:mysql://localhost:8066/test?" +
            "useUnicode=true&characterEncoding=UTF-8&" +
            "connectTimeout=3000&socketTimeout=30000";

    static {
        // First boot MyCat server
        System.setProperty("MYCAT_HOME", USER_DIR);
        System.setProperty("org.opencloudb.server.daemon", "true");
        String[] args = new String[]{};
        MycatStartup.main(args);
    }

    public void test() throws Exception {
        prepare();
        doTest();
        cleanup();
    }

    protected void prepare() {}

    protected void cleanup() {}

    protected abstract void doTest() throws Exception;

    protected static Connection getConnection() {
        try {
            return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
        } catch (SQLException e) {
            throw new AssertionError("Can't JDBC connection", e);
        }
    }

    public static void assertTrue(boolean b, String message) {
        if (!b) {
            throw new AssertionError(message);
        }
    }

    public static void assertFalse(boolean b, String message) {
        if (b) {
            throw new AssertionError(message);
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

    public static void assertNull(Object a, String message) {
        if (a != null) {
            throw new AssertionError(message);
        }
    }

    public static void assertNotNull(Object a, String message) {
        if (a == null) {
            throw new AssertionError(message);
        }
    }

}
