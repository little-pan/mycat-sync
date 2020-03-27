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
package org.opencloudb.mysql.handler;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.opencloudb.net.BackendConnection;
import org.opencloudb.net.mysql.ErrorPacket;
import org.slf4j.*;

/**
 * Heartbeat check for idle backend connections.
 * 
 * @author wuzhih
 * 
 */
public class ConnectionHeartBeatHandler implements ResponseHandler {

	private static final Logger log = LoggerFactory.getLogger(ConnectionHeartBeatHandler.class);

	protected final ReentrantLock lock = new ReentrantLock();
	private final ConcurrentHashMap<Long, HeartBeatCon> allCons = new ConcurrentHashMap<Long, HeartBeatCon>();

	public void heartBeat(BackendConnection conn, String sql) {
		log.debug("Heartbeat for idle backend {}: sql '{}'", conn, sql);

		try {
			HeartBeatCon hbCon = new HeartBeatCon(conn);
			HeartBeatCon oldCon = this.allCons.putIfAbsent(hbCon.conn.getId(), hbCon);
			if (oldCon == null) {
				conn.setResponseHandler(this);
				conn.query(sql);
			}
		} catch (Exception e) {
			executeException(conn, e);
		}
	}

	/**
	 * remove timeout connections
	 */
	public void abandonTimeoutConns() {
		if (this.allCons.isEmpty()) {
			return;
		}

		long curTime = System.currentTimeMillis();
		Iterator<Entry<Long, HeartBeatCon>> it = this.allCons.entrySet().iterator();
		while (it.hasNext()) {
			HeartBeatCon hbCon = it.next().getValue();
			if (hbCon.timeOutTimestamp < curTime) {
				hbCon.conn.close("heartbeat timeout");
				it.remove();
			}
		}
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		// not called
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		// not called
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		removeFinished(conn);
		try {
			ErrorPacket err = new ErrorPacket();
			err.read(data);
			log.warn("errorResponse: errno {}, errmsg {}",
						err.errno, new String(err.message));
		} finally {
			conn.release();
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExecute();
		if (executeResponse) {
			removeFinished(conn);
			conn.release();
		}
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {

	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		removeFinished(conn);
		conn.release();
	}

	private void executeException(BackendConnection c, Throwable e) {
		removeFinished(c);
		log.warn("Execute failed", e);
		c.close("heartbeat failed: " + e);
	}

	private void removeFinished(BackendConnection con) {
		this.allCons.remove(con.getId());
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		removeFinished(conn);
		log.warn("Connection closed reason {} in backend {}", reason, conn);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		log.debug("received field eof from backend {}", conn);
	}

}

class HeartBeatCon {
	public final long timeOutTimestamp;
	public final BackendConnection conn;

	public HeartBeatCon(BackendConnection conn) {
		this.timeOutTimestamp = System.currentTimeMillis() + 20 * 1000L;
		this.conn = conn;
	}

}
