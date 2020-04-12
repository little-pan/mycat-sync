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

import java.util.List;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.cache.CacheService;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.cache.CachePool;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.ServerSession;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.Callback;
import org.slf4j.*;

/** <p>
 * "from company where id=(select company_id from customer where id=3);" the one which
 * return data (id) is the datanode to store child table's records
 * </p>
 * @author wuzhih
 *
 * <p>
 * 1. Support non-blocking fetching;
 * 2. Support tx when the parent table row inserted in a tx, otherwise the data node can't be found;
 * 3. Parallel fetch.
 *
 * @since 1.5.1 2020-04-12
 * @author little-pan
 * </p>
 */
public class FetchChildTableStoreNodeHandler extends AbstractResponseHandler {

	static final Logger log = LoggerFactory.getLogger(FetchChildTableStoreNodeHandler.class);

	final ServerConnection source;
	final Callback<String> callback;

	private CachePool cache;
	private MycatServer server;
	private String cacheKey;

	private int nodeCount;
	private String result;
	private Throwable cause;
	private String dataNode;

	public FetchChildTableStoreNodeHandler(ServerConnection source, Callback<String> callback) {
		if (callback == null) {
			throw new NullPointerException("callback is null");
		}

		this.source = source;
		this.callback = callback;
	}

	public void execute(String schema, String sql, List<String> dataNodes) {
		this.nodeCount = dataNodes.size();
		if (this.nodeCount == 0) {
			String s = "dataNodes size must be bigger than 0: " + this.nodeCount;
			throw new IllegalArgumentException(s);
		}
		this.result = null;
		this.dataNode = null;
		this.cause = null;

		this.cacheKey = schema + ":" + sql;
		this.server = MycatServer.getContextServer();
		CacheService cacheService = this.server.getCacheService();
		this.cache = cacheService.getCachePool("ER_SQL2PARENTID");
		String dn = (String)this.cache.get(this.cacheKey);
		if (dn != null) {
			this.callback.call(dn, null);
			return;
		}

		int n = this.nodeCount;
		for (int i = 0; i < n; ++i) {
			dn = dataNodes.get(i);
			fetch(sql, dn);
		}
	}

	protected void fetch(String sql, String dn) {
		log.debug("Fetch store node by executing sql '{}' in node '{}'", sql, dn);

		int sqlType = ServerParse.SELECT;
		RouteResultsetNode rrn = new RouteResultsetNode(dn, sqlType, sql);
		// First try to get a bound conn from current session for support tx
		BackendConnection conn;
		boolean autoCommit = this.source.isAutocommit();
		if (!autoCommit) {
			ServerSession session = this.source.getSession();
			conn = session.getTarget(rrn);
			if (conn != null) {
				log.debug("Find a bound backend {}", conn);
				connectionAcquired(conn);
				return;
			}
		}

		try {
			MycatConfig conf = this.server.getConfig();
			PhysicalDBNode mysqlDN = conf.getDataNodes().get(dn);
			String schema = mysqlDN.getDatabase();
			mysqlDN.getConnection(schema, autoCommit, rrn, this, rrn);
		} catch (Throwable cause) {
			executeException(null, cause);
		}
	}

	private void tryComplete() {
		if (this.nodeCount != 0) {
			return;
		}

		String dn = this.dataNode;
		if (dn != null) {
			this.cache.putIfAbsent(this.cacheKey, dn);
		}
		this.callback.call(dn, this.cause);
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		try {
			RouteResultsetNode rrn = (RouteResultsetNode)conn.getAttachment();
			ServerSession session = this.source.getSession();
			session.bindConnection(rrn, conn);
			conn.setResponseHandler(this);
			conn.execute(rrn, this.source, this.source.isAutocommit());
		} catch (Throwable cause) {
			executeException(conn, cause);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		this.nodeCount--;
		try {
			if (conn != null) {
				conn.close("connection error: " + e);
			}
			log.warn("connection failed", e);
		} finally {
			tryComplete();
		}
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		this.nodeCount--;
		try {
			ServerSession session = this.source.getSession();
			session.releaseConnectionIfSafe(conn);

			ErrorPacket err = new ErrorPacket();
			err.read(data);
			String s = err.errno + ": " + new String(err.message);
			this.cause = new Exception(s);
			log.warn("errorResponse: {}", s);
		} finally {
			tryComplete();
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		boolean executed = conn.syncAndExecute();
		if (executed) {
			ServerSession session = this.source.getSession();
			session.releaseConnectionIfSafe(conn);
		}
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		String res = getColumn(row);
		log.debug("received rowResponse {} from backend {} ", res, conn);

		if (this.result == null) {
			RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
			this.result = res;
			this.dataNode = rrn.getName();
		} else {
			log.warn("Find multi data nodes for child table store");
		}
		// next row-eof/error response
	}

	private String getColumn(byte[] row) {
		RowDataPacket rowDataPkg = new RowDataPacket(1);
		rowDataPkg.read(row);
		byte[] columnData = rowDataPkg.fieldValues.get(0);
		return new String(columnData);
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		this.nodeCount--;
		try {
			ServerSession session = this.source.getSession();
			session.releaseConnectionIfSafe(conn);
		} finally {
			tryComplete();
		}
	}

	private void executeException(BackendConnection c, Throwable e) {
		this.nodeCount--;
		try {
			this.cause = e;
			log.debug("Fetching store node failed", e);
			if (c != null) {
				c.close("Fetching store node failed: " + e);
			}
		} finally {
			tryComplete();
		}
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		log.debug("Connection closed: reason '{}' in backend {}", reason, conn);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		// Ignore
	}

}