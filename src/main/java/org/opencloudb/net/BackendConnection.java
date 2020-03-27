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
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.backend.PhysicalDataSource;
import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.mysql.nio.MySQLDataSource;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public abstract class BackendConnection extends AbstractConnection implements ClosableConnection {

	private static final AtomicLong ID_GENERATOR = new AtomicLong();

	protected ResponseHandler respHandler;
	protected boolean isFinishConnect;
	protected PhysicalDataSource pool;

	protected int txIsolation;
	protected boolean autocommit;
	protected boolean borrowed = false;
	protected boolean modifiedSQLExecuted = false;
	protected boolean fromSlaveDB;
	private boolean isQuit;
	protected long lastTime;

	protected Object attachment;

	public BackendConnection(SocketChannel channel) {
		super(channel);
		this.lastTime = TimeUtil.currentTimeMillis();
		this.autocommit = true;
		this.isQuit = false;
	}

	@Override
	protected void wrapIoException(IOException cause) {
		throw new BackendException("Backend io error", cause);
	}

	public static long nextId() {
		return ID_GENERATOR.getAndIncrement();
	}

	public void discardClose(String reason){
		// 跨节点处理,中断后端连接时关闭
	}

	public abstract void onConnectFailed(Throwable e);

	public void finishConnect() throws IOException {
		SocketAddress sa = this.channel.getLocalAddress();
		this.localPort = ((InetSocketAddress)sa).getPort();
		this.isFinishConnect = true;
	}

	public PhysicalDataSource getPool() {
		return pool;
	}

	public void setPool(MySQLDataSource pool) {
		this.pool = pool;
	}

	public boolean isModifiedSQLExecuted() {
		return this.modifiedSQLExecuted;
	}

	public boolean isFromSlaveDB() {
		return fromSlaveDB;
	}

	public long getLastTime() {
		return lastTime;
	}

	public void setLastTime(long lastTime) {
		this.lastTime = lastTime;
	}

	public boolean isClosedOrQuit() {
		return isClosed() || this.isQuit;
	}

	public void setQuit (boolean quit) {
		this.isQuit = quit;
	}

	public Object getAttachment() {
		return attachment;
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public abstract void quit();

	public boolean setResponseHandler(ResponseHandler responseHandler) {
		this.respHandler = responseHandler;
		return true;
	}

	public ResponseHandler getResponseHandler() {
		return this.respHandler;
	}

	public abstract void query(String sql) throws UnsupportedOperationException;

	public abstract boolean syncAndExecute();

	public abstract void execute(RouteResultsetNode node, ServerConnection source, boolean autocommit);

	public abstract void commit() throws BackendException;

	public abstract void rollback() throws BackendException;

	public void recordSql(String host, String schema, String statement) {

	}

	public boolean isBorrowed() {
		return borrowed;
	}

	public void setBorrowed(boolean borrowed) {
		this.lastTime = TimeUtil.currentTimeMillis();
		this.borrowed = borrowed;
	}

	public int getTxIsolation() {
		return this.txIsolation;
	}

	public boolean isAutocommit() {
		return this.autocommit;
	}

	public void release() {
		// Deregister from current processor, so that this connection can run
		// in other processor synchronously at next time
		NioProcessor.ensureRunInProcessor();
		this.processKey.attach(null);
		this.processKey.cancel();
		this.processKey = null;
	}

	@Override
	public void close(String reason) {
		if (!this.isClosed.get()) {
			this.isQuit = true;
			super.close(reason);
			this.pool.connectionClosed(this);
			if (this.respHandler != null) {
				this.respHandler.connectionClose(this, reason);
				this.respHandler = null;
			}
		}
	}

	@Override
	public String toString() {
		return "BackendConnection [id=" + id + ", host=" + host + ", port="
				+ port + ", localPort=" + localPort + "]";
	}

}