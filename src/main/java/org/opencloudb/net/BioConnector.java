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
package org.opencloudb.net;

import org.opencloudb.MycatServer;
import org.opencloudb.util.NameableExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** The blocking connector.
 *
 * @author little-pan
 * @since 2020-03-21
 */
public class BioConnector implements SocketConnector {

    static final Logger log = LoggerFactory.getLogger(BioConnector.class);
    public static final AtomicLong ID_GENERATOR = new AtomicLong();

    protected final AtomicInteger connectCount = new AtomicInteger();
    protected final String name;
    protected final NameableExecutor executor;

    public BioConnector (NameableExecutor executor) {
        this("BioConnector", executor);
    }

    public BioConnector (String name, NameableExecutor executor) {
        this.name = name;
        this.executor = executor;
    }

    public Future<AbstractConnection> connect(final AbstractConnection c) {
        return this.executor.submit(new Callable<AbstractConnection>() {
            @Override
            public AbstractConnection call() throws IOException {
                log.debug("Connecting to {}:{}", c.host, c.port);
                SocketChannel channel = (SocketChannel) c.getChannel();
                channel.connect(new InetSocketAddress(c.host, c.port));
                finishConnect(c, channel);
                return c;
            }
        });
    }

    private void finishConnect(AbstractConnection c, SocketChannel channel) throws IOException {
        if (channel.isConnectionPending()) {
            channel.finishConnect();
        }
        log.debug("Connected to {}:{}", c.host, c.port);
        ConnectionManager manager = MycatServer.getInstance().getConnectionManager();
        c.setLocalPort(channel.socket().getLocalPort());
        c.setId(ID_GENERATOR.incrementAndGet());
        c.setManager(manager);
    }

}
