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

import org.apache.log4j.helpers.LogLog;
import org.opencloudb.config.ZkConfig;
import org.opencloudb.config.model.SystemConfig;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author mycat
 */
public final class MycatStartup {
    private static final String dateFormat = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) {
        //use zk ?
        ZkConfig.instance().initZk();

        try {
            String home = SystemConfig.getHomePath();
            if (home == null) {
                System.out.println(String.format("%s is not set.", SystemConfig.SYS_HOME));
                System.exit(-1);
            }

            // init
            MycatServer server = MycatServer.getInstance();
            server.beforeStart();
            // startup
            server.startup();
            System.out.println(MycatServer.NAME + " startup successfully. See logs in logs/mycat.log");
        } catch (Exception e) {
            SimpleDateFormat df = new SimpleDateFormat(dateFormat);
            LogLog.error(String.format("%s startup error", df.format(new Date())), e);
            System.exit(-1);
        }
    }

}
