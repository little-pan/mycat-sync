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
package org.opencloudb.mysql.nio;

import org.opencloudb.MycatServer;
import org.opencloudb.config.Capabilities;
import org.opencloudb.mysql.CharsetUtil;
import org.opencloudb.mysql.SecurityUtil;
import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.net.BackendException;
import org.opencloudb.net.ConnectionException;
import org.opencloudb.net.Handler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.HandshakePacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.Reply323Packet;
import org.slf4j.*;

/**
 * MySQL 验证处理器
 * 
 * @author mycat
 */
public class MySQLConnectionAuthenticator implements Handler {

	private static final Logger log = LoggerFactory.getLogger(MySQLConnectionAuthenticator.class);

	private final MySQLConnection source;
	private final ResponseHandler listener;

	public MySQLConnectionAuthenticator(MySQLConnection source, ResponseHandler listener)
			throws  NullPointerException {
		if (source == null) {
			throw new NullPointerException("source is null");
		}
		if (listener == null) {
			throw new NullPointerException("listener is null");
		}

		this.source = source;
		this.listener = listener;
	}

	public void connectionError(MySQLConnection source, Throwable e) {
		this.listener.connectionError(e, source);
	}

	@Override
	public void handle(byte[] data) {
		try {
			switch (data[4]) {
			case OkPacket.FIELD_COUNT:
				HandshakePacket packet = source.getHandshake();
				if (packet == null) {
					processHandShakePacket(data);
					// 发送认证数据包
					source.authenticate();
					break;
				}
				// 处理认证结果
				source.setHandler(new MySQLConnectionHandler(source));
				source.setAuthenticated(true);
				boolean clientCompress = Capabilities.CLIENT_COMPRESS ==
						(Capabilities.CLIENT_COMPRESS & packet.serverCapabilities);
				MycatServer server = MycatServer.getContextServer();
				boolean usingCompress = server.getConfig().getSystem().getUseCompression()==1 ;
				if(clientCompress&&usingCompress) {
					source.setSupportCompress(true);
				}
				this.listener.connectionAcquired(source);
				break;
			case ErrorPacket.FIELD_COUNT:
				ErrorPacket err = new ErrorPacket();
				err.read(data);
				String errMsg = new String(err.message);
				log.warn("Can't connect to mysql server: errmsg '{}', backend {}", errMsg, source);
				throw new ConnectionException(err.errno, errMsg);
			case EOFPacket.FIELD_COUNT:
				auth323(data[3]);
				break;
			default:
				packet = source.getHandshake();
				if (packet == null) {
					processHandShakePacket(data);
					// 发送认证数据包
					source.authenticate();
					break;
				} else {
					throw new BackendException("Unknown Packet");
				}
			}
		} catch (BackendException e) {
			connectionError(this.source, e);
		}
	}

	private void processHandShakePacket(byte[] data) {
		// 设置握手数据包
		HandshakePacket packet= new HandshakePacket();
		packet.read(data);
		source.setHandshake(packet);
		source.setThreadId(packet.threadId);

		// 设置字符集编码
		int charsetIndex = (packet.serverCharsetIndex & 0xff);
		String charset = CharsetUtil.getCharset(charsetIndex);
		if (charset != null) {
			source.setCharset(charset);
		} else {
			throw new BackendException("Unknown charsetIndex: " + charsetIndex);
		}
	}

	private void auth323(byte packetId) {
		// 发送323响应认证数据包
		Reply323Packet r323 = new Reply323Packet();
		r323.packetId = ++packetId;
		String pass = source.getPassword();
		if (pass != null && pass.length() > 0) {
			String seed = new String(source.getHandshake().seed);
			r323.seed = SecurityUtil.scramble323(pass, seed).getBytes();
		}
		r323.write(source);
	}

}