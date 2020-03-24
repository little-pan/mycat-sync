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
package org.opencloudb.jdbc;

import org.opencloudb.MycatServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.Executor;

/** The JDBC backend heartbeat caller.
 *
 * @author little-pan
 * @since 2020-03-24
 */
public class JDBCHeartbeatCaller {

    static final Logger log = LoggerFactory.getLogger(JDBCHeartbeatCaller.class);

    private final boolean useJdbc4Validation;
    private final String sql;

    private boolean networkTimeoutSupported = true;
    private int networkTimeout;

    public JDBCHeartbeatCaller(String sql) {
        this.sql = sql;
        this.useJdbc4Validation = sql.length() == 0;
    }

    public boolean isUseJdbc4Validation() {
        return useJdbc4Validation;
    }

    public boolean call (Connection c, int heartbeatTimeout) throws SQLException {
        try {
            setNetworkTimeout(c, heartbeatTimeout);

            int heartbeatSeconds = (int) Math.max(1000, heartbeatTimeout) / 1000;
            if (this.useJdbc4Validation) {
                log.debug("jdbc4 validate '{}'", c);
                if (!c.isValid(heartbeatSeconds)) {
                    // ER
                    log.error("JDBCHeartBeat error: invalid '{}' by jdbc4", c);
                    return false;
                }
                // OK
            } else {
                try (Statement s = c.createStatement()) {
                    if (!this.networkTimeoutSupported) {
                        s.setQueryTimeout(heartbeatSeconds);
                    }
                    s.execute(sql);
                }
                if (!c.getAutoCommit()) {
                    c.commit();
                }
                // OK
            }
            return true;
        } finally {
            setNetworkTimeout(c, this.networkTimeout);
        }
    }

    private void setNetworkTimeout (Connection c, int timeout) throws SQLException {
        if (this.networkTimeoutSupported) {
            Executor executor = MycatServer.getInstance().getBusinessExecutor();
            try {
                log.debug("set connection networkTimeout to {}ms", timeout);
                this.networkTimeout = c.getNetworkTimeout();
                c.setNetworkTimeout(executor, timeout);
            } catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
                this.networkTimeoutSupported = false;
                log.warn("setNetworkTimeout() is unsupported in connection '{}'", c);
            }
        }
    }

}
