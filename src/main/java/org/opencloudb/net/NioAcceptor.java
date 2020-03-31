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
package org.opencloudb.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.opencloudb.MycatServer;
import org.opencloudb.net.factory.FrontendConnectionFactory;
import org.opencloudb.util.IoUtil;
import org.slf4j.*;

/**
 * @author mycat
 */
public final class NioAcceptor extends Thread  implements SocketAcceptor, AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(NioAcceptor.class);

	private final MycatServer server;

	private final int port;
	private final Selector selector;
	private final ServerSocketChannel serverChannel;
	private final FrontendConnectionFactory factory;
	private long acceptCount;
	private final NioProcessorPool processorPool;

	private final CountDownLatch startLatch;
	private volatile boolean stopped;

	public NioAcceptor(String name, String bindIp, int port, FrontendConnectionFactory factory,
					   NioProcessorPool processorPool) throws IOException {
		super.setName(name);
		this.server = MycatServer.getContextServer();
		if (this.server == null) {
			throw new IllegalStateException("Context server is null");
		}
		this.port = port;
		this.selector = Selector.open();

		boolean failed = true;
		try {
			this.serverChannel = ServerSocketChannel.open();
			try {
				this.serverChannel.configureBlocking(false);
				/** 设置TCP属性 */
				this.serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
				this.serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 32 << 10);
				this.serverChannel.bind(new InetSocketAddress(bindIp, port), 100);
				this.serverChannel.register(this.selector, SelectionKey.OP_ACCEPT);

				this.factory = factory;
				this.processorPool = processorPool;
				this.startLatch = new CountDownLatch(1);
				failed = false;
			} finally {
				if (failed) {
					IoUtil.close(this.serverChannel);
				}
			}
		} finally {
			if (failed) {
				IoUtil.close(this.selector);
			}
		}
	}

	public int getPort() {
		return port;
	}

	public long getAcceptCount() {
		return acceptCount;
	}

	@Override
	public void run() {
		try {
			MycatServer.setContextServer(this.server);
			final Selector selector = this.selector;
			SocketAddress sa = this.serverChannel.getLocalAddress();
			log.info("{} started and listen on {}", this.getName(), sa);
			this.startLatch.countDown();

			for (; !this.stopped; ) {
				++this.acceptCount;
				selector.select();
				Set<SelectionKey> keys = selector.selectedKeys();
				try {
					for (SelectionKey key : keys) {
						if (key.isValid() && key.isAcceptable()) {
							accept();
						} else {
							key.cancel();
						}
					}
				} finally {
					keys.clear();
				}
			}
			log.info("exit normally");
		} catch (IOException e) {
			log.error("Accept loop failed", e);
		} finally {
			this.startLatch.countDown();
			IoUtil.close(this.serverChannel);
			IoUtil.close(this.selector);
			MycatServer.removeContextServer();
		}
	}

	private void accept() {
		SocketChannel ch = null;
		try {
			ch = this.serverChannel.accept();
			ch.configureBlocking(false);

			FrontendConnection c = this.factory.make(ch);
			c.setAccepted(true);
			c.setId(NioProcessor.nextId());
			
			NioProcessor processor = this.processorPool.getNextProcessor();
			processor.accept(c);
		} catch (Throwable cause) {
			IoUtil.close(ch);
	        log.warn(getName() + ": accept a connection failed", cause);
		}
	}

	public boolean awaitStarted() {
		try {
			this.startLatch.await();
			return true;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	@Override
	public void close() {
		this.stopped = true;
		this.selector.wakeup();
	}

}