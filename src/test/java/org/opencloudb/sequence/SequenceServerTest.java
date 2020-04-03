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

public class SequenceServerTest extends BaseServerTest {

    public static void main(String[] args) throws Exception {
        new SequenceServerTest().test();
    }

    @Override
    protected void doTest() throws Exception {
        testNextValue();
    }

    private void testNextValue() throws Exception {
        String sql = "select NEXT VALUE FOR MYCATSEQ_COMPANY";
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            assertTrue(rs.next(), "No resultSet");
            long a = rs.getLong(1);
            assertFalse(rs.next(), "More resultSet");
            rs.close();

            rs = stmt.executeQuery(sql);
            assertTrue(rs.next(), "No resultSet");
            long b = rs.getLong(1);
            assertFalse(rs.next(), "More resultSet");
            rs.close();

            assertTrue(a + 1 == b, "Next value error: a  = " + a + ", b = " + b);
        }
    }

}
