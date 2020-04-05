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
package org.opencloudb.server;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatPrivileges;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.factory.FrontendConnectionFactory;
import org.opencloudb.server.handler.ServerLoadDataInfileHandler;

/**
 * @author mycat
 */
public class ServerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(SocketChannel channel) throws IOException {
        MycatServer server = MycatServer.getContextServer();
        SystemConfig sys = server.getConfig().getSystem();
        ServerConnection source = new ServerConnection(channel);
        MycatConfig conf = server.getConfig();

        conf.setSocketParams(source, true);
        source.setPrivileges(MycatPrivileges.instance());
        source.setQueryHandler(new ServerQueryHandler(source));
        source.setLoadDataInfileHandler(new ServerLoadDataInfileHandler(source));
        source.setTxIsolation(sys.getTxIsolation());
        source.setSession(new ServerSession(source));

        return source;
    }

}