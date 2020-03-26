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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.BackendException;
import org.opencloudb.util.IoUtil;
import org.slf4j.*;

/**
 * Nio processor that handles network read/write events.
 * 
 * @author mycat
 */
public final class NioProcessor extends AbstractProcessor {

	private static final Logger log = LoggerFactory.getLogger(NioProcessor.class);

	private static final ThreadLocal<MycatServer> LOCAL_SERVER = new ThreadLocal<>();
	private static final ThreadLocal<NioProcessor> LOCAL_PROCESSOR = new ThreadLocal<>();
	/** Backend connection ID generator. */
	public static final AtomicLong BC_ID_GENERATOR = new AtomicLong();

	private final MycatServer server;

	private final Selector selector;
	private final Queue<AbstractConnection> registerQueue;
	private long processCount;
	private final Queue<Runnable> taskQueue;
	private long connectCount;

	private volatile boolean shutdown;
	private Thread processThread;

	public NioProcessor(String name) throws IOException {
		super(name);
		this.server = MycatServer.getContextServer();
		if (this.server == null) {
			throw new NullPointerException("Context server is null");
		}

		Selector selector = Selector.open();
		boolean failed = true;
		try {
			this.selector = selector;
			this.registerQueue = new ConcurrentLinkedQueue<>();
			this.taskQueue = new ConcurrentLinkedQueue<>();
			failed = false;
		} finally {
			if (failed) {
				IoUtil.close(selector);
			}
		}
	}

	public static MycatServer contextServer() {
		return LOCAL_SERVER.get();
	}

	public MycatServer getServer () {
		return this.server;
	}

	public void execute(Runnable task) {
		if (currentProcessor() == this) {
			task.run();
		} else {
			this.taskQueue.offer(task);
			this.selector.wakeup();
		}
	}

	public static void runInProcessor(Runnable task) {
		if (currentProcessor() == null) {
			// Async run in processor if current thread not a processor thread
			MycatServer server = MycatServer.getContextServer();
			NioProcessorPool processorPool = server.getProcessorPool();
			processorPool.getNextProcessor().execute(task);
		} else {
			// Sync run if current thread is a processor thread
			task.run();
		}
	}

	public static NioProcessor ensureRunInProcessor() {
		NioProcessor processor = currentProcessor();
		if (processor == null) {
			throw new IllegalStateException("Current thread not a processor thread");
		}
		return processor;
	}

	public static NioProcessor ensureRunInProcessor(NioProcessor processor) {
		final NioProcessor currentProcessor = currentProcessor();
		if (currentProcessor == null || currentProcessor != processor) {
			throw new IllegalStateException("Current thread not the given processor thread");
		}
		return currentProcessor;
	}

	public static NioProcessor currentProcessor() {
		return LOCAL_PROCESSOR.get();
	}

	final void startup() {
		this.processThread = new Thread(this, this.name);
		this.processThread.start();
	}

	protected void shutdown() {
		this.shutdown = true;
		this.selector.wakeup();
	}

	public boolean isShutdown() {
		return this.shutdown;
	}

	@Override
	public void run() {
		if (!isProcessorThread()) {
			throw new IllegalStateException("Run processor in non-processor thread");
		}

		try {
			LOCAL_SERVER.set(this.server);
			LOCAL_PROCESSOR.set(this);

			Selector selector = this.selector;
			for (;;) {
				++this.processCount;
				int n = selector.select();
				// 1. process IO task
				processTasks();
				// 2. Process IO event
				if (n > 0) {
					Set<SelectionKey> keys = selector.selectedKeys();
					try {
						for (SelectionKey key: keys) {
							processEvents(key);
						}
					} finally {
						keys.clear();
					}
				}

				if (isShutdown() && selector.keys().size() == 0) {
					break;
				}
			}
			log.info("exit normally");
		} catch (IOException e) {
			log.error(this.name +": process failed", e);
		} finally {
			IoUtil.close(this.selector);
			LOCAL_PROCESSOR.remove();
			LOCAL_SERVER.remove();
		}
	}

	private void processEvents(SelectionKey key) {
		Object att = key.attachment();
		if (att == null || !key.isValid()) {
			key.cancel();
			return;
		}

		AbstractConnection con = (AbstractConnection) att;
		try {
			if (key.isConnectable()) {
				++this.connectCount;
				finishConnect(key, con);
				return;
			}

			if (key.isValid() && key.isReadable()) {
				con.onRead();
			}
			if (key.isValid() && key.isWritable()) {
				con.onWrite();
			}
		} catch (Throwable e) {
			log.warn("Process failed", e);
			con.close("Process failed: " + e.getCause());
		}
	}

	private void processTasks() {
		// 1. register accepted
		for (;;) {
			AbstractConnection con = this.registerQueue.poll();
			if (con == null) {
				break;
			}
			register(con, true);
		}

		// 2. run task such as register connection
		for (;;) {
			Runnable task = this.taskQueue.poll();
			if (task == null) {
				break;
			}
			try {
				task.run();
			} catch (Throwable cause) {
				log.warn("Execute task failed", cause);
			}
		}
	}

	public void register (BackendConnection con, boolean autoRead) {
		register((AbstractConnection)con, autoRead);
	}

	public void register (AbstractConnection con, boolean autoRead) {
		ensureRunInProcessor(this);
		try {
			con.onRegister(this.selector, autoRead);
		} catch (Throwable e) {
			log.warn("Register failed", e);
			con.close("Register failed: " + e.getCause());
			if (e instanceof BackendException) {
				BackendConnection bc = (BackendConnection)con;
				bc.getResponseHandler().connectionError(e.getCause(), null);
			}
		}
	}

	public boolean isOpen () {
		return this.selector.isOpen();
	}

	private void finishConnect(SelectionKey key, AbstractConnection con) {
		NioBackendConnection c = (NioBackendConnection) con;
		try {
			if (finishConnect(c, c.channel)) {
				key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
				c.setId(BC_ID_GENERATOR.incrementAndGet());
				ConnectionManager manager = contextServer().getConnectionManager();
				c.setManager(manager);
				register(con, true);
			}
		} catch (IOException e) {
			clearSelectionKey(key);
			c.close("Connection failed: " + e);
			c.onConnectFailed(e);
		}
	}

	private boolean finishConnect(AbstractConnection c, SocketChannel channel)
			throws IOException {
		if (channel.isConnectionPending()) {
			channel.finishConnect();
			NioBackendConnection nc = (NioBackendConnection)c;
			nc.finishConnect();
			return true;
		} else {
			return false;
		}
	}

	final void postRegister(AbstractConnection c) throws IllegalStateException {
		if (!isOpen()) {
			throw new IllegalStateException("Processor has closed");
		}

		this.registerQueue.offer(c);
		this.selector.wakeup();
	}

	private void clearSelectionKey(SelectionKey key) {
		if (key.isValid()) {
			key.attach(null);
			key.cancel();
		}
	}

	// Called in backend connection factory
	public void connect(AbstractConnection c) {
		try {
			ensureRunInProcessor(this);

			SocketChannel channel = c.getChannel();
			channel.register(this.selector, SelectionKey.OP_CONNECT, c);
			channel.connect(new InetSocketAddress(c.host, c.port));
			this.selector.wakeup();
		} catch (IOException e) {
			throw new BackendException("Connect failed", e);
		}
	}

	public boolean join() {
		try {
			if (this.processThread != null) {
				this.processThread.join();
			}
			return true;
		} catch (InterruptedException e) {
			this.processThread.interrupt();
			return false;
		}
	}

	final Queue<AbstractConnection> getRegisterQueue() {
		return this.registerQueue;
	}

	final long getProcessCount() {
		return this.processCount;
	}

	public long getConnectCount() {
		return connectCount;
	}

}
