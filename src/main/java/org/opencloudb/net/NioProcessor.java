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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.MycatServer;
import org.opencloudb.statistic.CommandCount;
import org.opencloudb.util.IoUtil;
import org.slf4j.*;

/**
 * Nio processor that handles network read/write events.
 * 
 * @author mycat
 */
public final class NioProcessor extends AbstractProcessor {

	private static final Logger log = LoggerFactory.getLogger(NioProcessor.class);

	private static final ThreadLocal<NioProcessor> LOCAL_PROCESSOR = new ThreadLocal<>();
	private static final AtomicLong ID_GENERATOR = new AtomicLong();
	private static final String IO_RATIO_PROP = "org.opencloudb.net.ioRatio";
	private static final int ioRatio = Integer.getInteger(IO_RATIO_PROP, 50);

	static {
		if (ioRatio <= 0 || ioRatio > 100) {
			String s = "'"+IO_RATIO_PROP+"' should be in (1, 100], actually is " + ioRatio;
			throw new ExceptionInInitializerError(s);
		}
	}

	private final MycatServer server;

	private final Selector selector;
	private long processCount;
	private final Queue<Runnable> taskQueue;
	private long connectCount;

	// Statistics
	private final AtomicInteger queueSize;
	private long taskCount;
	private boolean active;
	private final CommandCount commands;
	private final AtomicInteger frontendCount;
	private long netInBytes;
	private long netOutBytes;

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
			this.queueSize = new AtomicInteger();
			this.taskQueue = new ConcurrentLinkedQueue<>();
			this.commands = new CommandCount();
			this.frontendCount = new AtomicInteger();
			failed = false;
		} finally {
			if (failed) {
				IoUtil.close(selector);
			}
		}
	}

	public static MycatServer contextServer() {
		return MycatServer.getContextServer();
	}

	public MycatServer getServer () {
		return this.server;
	}

	public void execute(Runnable task) {
		if (currentProcessor() == this) {
			task.run();
		} else {
			executeLater(task);
		}
	}

	public void executeLater(Runnable task) {
		this.taskQueue.offer(task);
		this.queueSize.incrementAndGet();
		this.taskCount++;
		this.selector.wakeup();
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

	static long nextId () {
		return ID_GENERATOR.getAndIncrement();
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
			MycatServer.setContextServer(this.server);
			LOCAL_PROCESSOR.set(this);

			Selector selector = this.selector;
			for (;;) {
				++this.processCount;
				this.active = false;
				int n = selector.select();
				this.active = true;

				if (ioRatio == 100) {
					try {
						processEvents(n);
					} finally {
						processTasks(0);
					}
				} else {
					long startTime = System.nanoTime();
					try {
						processEvents(n);
					} finally {
						long ioTime = System.nanoTime() - startTime;
						long timeoutNanos = ioTime * (100 - ioRatio) / ioRatio;
						processTasks(timeoutNanos);
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
			cleanup();
		}
	}

	private void cleanup() {
		this.active = false;
		cleanupFrontends();
		IoUtil.close(this.selector);
		cleanupTaskQueue();
		LOCAL_PROCESSOR.remove();
		MycatServer.removeContextServer();
	}

	private void cleanupFrontends() {
		if (!isOpen()) {
			log.debug("{} has been closed", this.name);
			return;
		}
		Set<SelectionKey> keys = this.selector.keys();
		for (SelectionKey key: keys) {
			try {
				Object att = key.attachment();
				if (att instanceof FrontendConnection) {
					FrontendConnection fc = (FrontendConnection) att;
					fc.close(this.name + " is closing");
				}
			} catch (Throwable cause) {
				log.error("Fatal: close frontend connection", cause);
			}
		}
	}

	private void cleanupTaskQueue() {
		for (;;) {
			final Runnable task = this.taskQueue.poll();
			if (task == null) {
				break;
			}
			if (task instanceof AcceptTask) {
				AcceptTask at = (AcceptTask)task;
				at.con.close("Processor has been closed");
			}
		}
	}

	private void processEvents(int keyCount) {
		if (keyCount <= 0) {
			return;
		}

		Set<SelectionKey> keys = selector.selectedKeys();
		try {
			for (final SelectionKey key: keys) {
				Object att = key.attachment();
				if (att == null || !key.isValid()) {
					key.cancel();
					continue;
				}

				AbstractConnection con = (AbstractConnection) att;
				try {
					if (key.isConnectable()) {
						++this.connectCount;
						finishConnect(key, con);
						continue;
					}

					if (key.isValid() && key.isReadable()) {
						con.onRead();
					}
					if (key.isValid() && key.isWritable()) {
						con.onWrite();
					}
				} catch (Throwable e) {
					log.warn("Process failed in conn " + con, e);
					Throwable cause = e.getCause();
					if (cause == null) {
						cause = e;
					}
					con.close("Process failed: " + cause);
				}
			} // rof
		} finally {
			keys.clear();
		}
	}

	private void processTasks(final long timeoutNanos) {
		final long expires = System.nanoTime() + timeoutNanos;
		long runTasks = 0;

		for (;;) {
			Runnable task = this.taskQueue.poll();
			if (task == null) {
				break;
			}

			try {
				this.queueSize.decrementAndGet();
				task.run();
			} catch (Throwable cause) {
				log.warn("Execute task failed", cause);
			}

			if ((timeoutNanos > 0)
					&& ((++runTasks & 0x3F) == 0)
					&& (System.nanoTime() >= expires)) {
				break;
			}
		}
	}

	public void register (BackendConnection con, boolean autoRead) {
		register((AbstractConnection)con, autoRead);
	}

	protected void register (AbstractConnection con, boolean autoRead) {
		ensureRunInProcessor(this);
		try {
			if (!isOpen()) {
				con.close("Processor has closed");
				return;
			}

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
		BackendConnection c = (BackendConnection) con;
		try {
			if (finishConnect(c, c.channel)) {
				key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
				c.setId(BackendConnection.nextId());
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
			BackendConnection nc = (BackendConnection)c;
			nc.finishConnect();
			return true;
		} else {
			return false;
		}
	}

	final void accept(final AbstractConnection c) throws IllegalStateException {
		if (!isOpen()) {
			throw new IllegalStateException(this.name + " has been closed");
		}

		AcceptTask postTask = new AcceptTask(c);
		execute(postTask);
	}

	class AcceptTask implements Runnable {

		final AbstractConnection con;

		public AcceptTask(AbstractConnection con) {
			this.con = con;
		}

		@Override
		public void run() {
			boolean failed = true;
			try {
				MycatServer server = contextServer();
				ConnectionManager manager = server.getConnectionManager();
				this.con.setManager(manager);
				register(this.con, true);
				failed = false;
			} finally {
				if (failed) {
					this.con.close("Fatal: register connection");
				}
			}
		}

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

	@Override
	public boolean isActive() {
		return this.active;
	}

	public int getQueueSize() {
		return this.queueSize.get();
	}

	public long getTaskCount() {
		return this.taskCount;
	}

	public long getCompletedTaskCount() {
		return (getTaskCount() - getQueueSize());
	}

	public CommandCount getCommands() {
		return this.commands;
	}

	public AtomicInteger getFrontendCount() {
		return this.frontendCount;
	}

	final long getProcessCount() {
		return this.processCount;
	}

	public long getConnectCount() {
		return this.connectCount;
	}

	public long getNetInBytes() {
		return this.netInBytes;
	}

	public void addNetInBytes(long bytes) {
		this.netInBytes += bytes;
	}

	public long getNetOutBytes() {
		return this.netOutBytes;
	}

	public void addNetOutBytes(long bytes) {
		this.netOutBytes += bytes;
	}

	public int getWriteQueueSize() {
		ConnectionManager manager = contextServer().getConnectionManager();
		return manager.getWriteQueueSize(this);
	}

	public int getFrontendSize() {
		ConnectionManager manager = contextServer().getConnectionManager();
		return manager.getFrontendSize(this);
	}

	public int getBackendSize() {
		ConnectionManager manager = contextServer().getConnectionManager();
		return manager.getBackendSize(this);
	}

}
