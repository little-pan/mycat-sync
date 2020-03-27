/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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

import org.opencloudb.config.ZkConfig;
import org.opencloudb.config.model.SystemConfig;

import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.String.*;

/**
 * @author mycat
 */
public final class MycatStartup {

    public static void main(String[] args) {
        // use zk ?
        ZkConfig.instance().initZk();

        try {
            String home = SystemConfig.getHomePath();
            if (home == null) {
                System.err.println(format("%s is not set.", SystemConfig.SYS_HOME));
                System.exit(-1);
            }

            // init
            MycatServer server = new MycatServer();
            server.beforeStart();
            // startup
            server.startup();
            System.out.println(format("%s startup success. See logs in '%s/logs/mycat.log'", MycatServer.NAME, home));
        } catch (Throwable e) {
            printError(e);
            System.exit(-1);
        }
    }

    private static void printError(Throwable e) {
        String threadName = Thread.currentThread().getName();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String datetime = df.format(new Date());
        synchronized (System.err) {
            System.err.printf("%s [%s] %s startup error: ", datetime, threadName, MycatServer.NAME);
            e.printStackTrace(System.err);
        }
    }

}
