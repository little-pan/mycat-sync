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

import org.opencloudb.sequence.SequenceServerTest;
import org.opencloudb.server.handler.LoadDataInfileHandlerServerTest;

import java.util.ArrayList;
import java.util.List;

public class AllServerTest extends BaseServerTest {

    public static void main(String[] args) throws Exception {
        new AllServerTest().test();
    }

    @Override
    protected void prepare() {
        super.prepare();
        // Add all here
        int rounds = 1;
        add(new LoadDataInfileHandlerServerTest(rounds)).
                add(new SequenceServerTest(rounds));
    }

    @Override
    public void doTest() throws Exception {
        int n = 0, total = this.tests.size();
        try {
            for (BaseServerTest test : this.tests) {
                test.test();
                ++n;
            }
        } finally {
            String testCase = getClass().getSimpleName();
            info("%s: tested cases %s, total cases %s", testCase, n, total);
        }
    }

    @Override
    protected void cleanup() {
        this.tests.clear();
        super.cleanup();
    }

    protected AllServerTest add(BaseServerTest test) {
        this.tests.add(test);
        return this;
    }

    final List<BaseServerTest> tests = new ArrayList<>();

}
