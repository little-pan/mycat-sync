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
package org.opencloudb.response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opencloudb.MycatServer;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.ConnectionManager;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.util.SplitUtil;
import org.slf4j.*;

/**
 * @author mycat
 */
public final class KillConnection {

    private static final Logger log = LoggerFactory.getLogger(KillConnection.class);

    public static void response(String stmt, int offset, ManagerConnection mc) {
        List<FrontendConnection> list = getList(stmt, offset);
        int count = 0;

        for (FrontendConnection c : list) {
            log.debug("{} killed by manager", c);
            c.close("kill by manager");
            count++;
        }

        OkPacket packet = new OkPacket();
        packet.packetId = 1;
        packet.affectedRows = count;
        packet.serverStatus = 2;
        packet.write(mc);
    }

    private static List<FrontendConnection> getList(String stmt, int offset) {
        String ids = stmt.substring(offset).trim();

        if (ids.length() > 0) {
            String[] idList = SplitUtil.split(ids, ',', true);
            List<FrontendConnection> fcList = new ArrayList<>(idList.length);
            ConnectionManager manager = MycatServer.getInstance().getConnectionManager();
            for (String id : idList) {
                long value;
                try {
                    value = Long.parseLong(id);
                } catch (NumberFormatException e) {
                    continue;
                }
                FrontendConnection fc;
                if ((fc = manager.getFrontends().get(value)) != null) {
                    fcList.add(fc);
                }
            }
            return fcList;
        }

        return Collections.emptyList();
    }

}