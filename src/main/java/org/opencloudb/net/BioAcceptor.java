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
import org.opencloudb.net.factory.FrontendConnectionFactory;
import org.opencloudb.util.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/** The blocking acceptor of a front-end connection.
 *
 * @author little-pan
 * @since 2020-03-21
 */
public class BioAcceptor extends Thread implements SocketAcceptor, AutoCloseable {

    static final Logger log = LoggerFactory.getLogger(BioAcceptor.class);
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    protected final String name;
    protected final int port;
    protected final ServerSocketChannel serverChannel;
    protected final FrontendConnectionFactory factory;
    protected final BioProcessorPool processorPool;
    private long acceptCount;

    private final CountDownLatch startLatch = new CountDownLatch(1);

    public BioAcceptor (String bindIp, int port,
                        FrontendConnectionFactory factory, BioProcessorPool processorPool) throws IOException {
        this("BioAcceptor", false, bindIp, port, factory, processorPool);
    }

    public BioAcceptor (String name, String bindIp, int port,
                        FrontendConnectionFactory factory, BioProcessorPool processorPool) throws IOException {
        this(name, false, bindIp, port, factory, processorPool);
    }

    public BioAcceptor (String name, boolean daemon, String bindIp, int port,
                        FrontendConnectionFactory factory, BioProcessorPool processorPool) throws IOException {
        super(name);
        this.name = name;
        super.setDaemon(daemon);

        this.serverChannel = ServerSocketChannel.open();
        this.port = port;
        boolean failed = true;
        try {
            this.serverChannel.configureBlocking(true);
            /** 设置TCP属性 */
            this.serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            this.serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);
            // backlog = 100
            this.serverChannel.bind(new InetSocketAddress(bindIp, port), 100);
            failed = false;
        } finally {
            if (failed) {
                IoUtil.close(this.serverChannel);
            }
        }

        this.factory = factory;
        this.processorPool = processorPool;
    }

    @Override
    public void run() {
        try {
            SocketAddress sa = this.serverChannel.getLocalAddress();
            log.info("{} started and listen on {}", this.name, sa);

            this.startLatch.countDown();
            for (;this.serverChannel.isOpen();) {
                SocketChannel ch = accept();
                if (ch != null) {
                    ++this.acceptCount;
                }
            }
            log.info("{} quit", this.name);
        } catch (final IOException e) {
            log.warn("Accept failed", e);
            close();
        } finally {
            this.startLatch.countDown();
        }
    }

    public void awaitStarted () throws InterruptedException {
        this.startLatch.await();
    }

    private SocketChannel accept() throws IOException {
        final SocketChannel channel = this.serverChannel.accept();
        try {
            log.debug("Accept a connection: {}", channel);
            if (channel == null) {
                return null;
            }
            channel.configureBlocking(true);
            FrontendConnection c = this.factory.make(channel);
            ConnectionManager manager = MycatServer.getInstance().getConnectionManager();
            c.setAccepted(true);
            c.setId(ID_GENERATOR.incrementAndGet());
            c.setManager(manager);
            postConnect(c);
        } catch (final Throwable e) {
            closeChannel(channel);
            log.warn(getName() + " connection failed", e);
        }

        return channel;
    }

    private void postConnect(FrontendConnection c) {
        this.processorPool.execute(new BioProcessor(c));
    }

    private void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        Socket socket = channel.socket();
        if (socket != null) {
            IoUtil.close(socket);
        }
        IoUtil.close(channel);
    }

    @Override
    public void close() {
        log.info("{} close", this.name);
        IoUtil.close(this.serverChannel);
    }

    @Override
    public int getPort() {
        return this.port;
    }

    public long getAcceptCount() {
        return this.acceptCount;
    }

}
