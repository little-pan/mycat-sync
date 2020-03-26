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

import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.BackendException;

/**
 * @author mycat
 */
public abstract class NioBackendConnection extends AbstractConnection implements BackendConnection {

	protected boolean isFinishConnect;

	public NioBackendConnection(SocketChannel channel) {
		super(channel);
	}

	@Override
	protected void wrapIoException(IOException cause) {
		throw new BackendException("Backend io error", cause);
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
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

	@Override
	public void release() {
		// Deregister from current processor, so that this connection can run
		// in other processor synchronously at next time
		NioProcessor.ensureRunInProcessor();
		this.processKey.attach(null);
		this.processKey.cancel();
		this.processKey = null;
	}

	@Override
	public String toString() {
		return "NioBackendConnection [id=" + id + ", host=" + host + ", port="
				+ port + ", localPort=" + localPort + "]";
	}

}