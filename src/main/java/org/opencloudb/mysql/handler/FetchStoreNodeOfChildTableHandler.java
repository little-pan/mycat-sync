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
import org.opencloudb.net.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.cache.CachePool;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.Callback;
import org.slf4j.*;

/**
 * "from company where id=(select company_id from customer where id=3);" the one which
 * return data (id) is the datanode to store child table's records
 * 
 * @author wuzhih
 * 
 */
public class FetchStoreNodeOfChildTableHandler extends AbstractResponseHandler {

	private static final Logger log = LoggerFactory.getLogger(FetchStoreNodeOfChildTableHandler.class);

	private final Callback<String> callback;
	private CachePool cache;
	private MycatServer server;

	private String sql;
	private String cacheKey;
	private List<String> dataNodes;
	private int curNodeIndex;
	private String result;
	private Throwable cause;
	private String dataNode;

	public FetchStoreNodeOfChildTableHandler(Callback<String> callback) {
		if (callback == null) {
			throw new NullPointerException("callback is null");
		}

		this.callback = callback;
	}

	public void execute(String schema, String sql, List<String> dataNodes) {
		this.cacheKey = schema + ":" + sql;
		this.server = MycatServer.getContextServer();
		this.cache = this.server.getCacheService().getCachePool("ER_SQL2PARENTID");
		String dn = (String)this.cache.get(this.cacheKey);
		if (dn != null) {
			this.callback.call(dn, null);
			return;
		}
		this.sql = sql;
		this.dataNodes = dataNodes;
		this.curNodeIndex = 0;
		log.debug("Find child node with sql '{}'", this.sql);
		fetch();
	}

	protected void fetch() {
		boolean ok = this.dataNode != null;

		if (ok || this.curNodeIndex >= this.dataNodes.size()) {
			if (ok) {
				this.cache.putIfAbsent(this.cacheKey, dataNode);
			}
			this.callback.call(this.dataNode, this.cause);
			return;
		}

		MycatConfig conf = this.server.getConfig();
		String dn = this.dataNodes.get(this.curNodeIndex++);
		PhysicalDBNode mysqlDN = conf.getDataNodes().get(dn);
		try {
			log.debug("execute in datanode '{}'", dn);
			mysqlDN.getConnection(mysqlDN.getDatabase(), true,
					new RouteResultsetNode(dn, ServerParse.SELECT, this.sql),
					this, dn);
		} catch (Throwable cause) {
			this.cause = cause;
			fetch();
		}
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		try {
			conn.setResponseHandler(this);
			conn.query(this.sql);
		} catch (Exception e) {
			executeException(conn, e);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		try {
			if (conn != null) conn.close("connectionError " + e);
			log.warn("connection failed", e);
		} finally {
			fetch();
		}
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		try {
			try {
				ErrorPacket err = new ErrorPacket();
				err.read(data);
				log.warn("errorResponse: errno {}, errmsg {} ", err.errno, new String(err.message));
			} finally {
				conn.release();
			}
		} finally {
			fetch();
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExecute();
		try {
			if (executeResponse) {
				conn.release();
			}
		} finally {
			fetch();
		}
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		String res = getColumn(row);
		log.debug("received rowResponse: response {} from backend {} ", res, conn);

		if (this.result == null) {
			this.result = res;
			this.dataNode = (String) conn.getAttachment();
		} else {
			log.warn("Find multi data nodes for child table store, sql is '{}'", this.sql);
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
		try {
			fetch();
		} finally {
			conn.release();
		}
	}

	private void executeException(BackendConnection c, Throwable e) {
		try {
			this.cause = e;
			log.debug("Fetching store node failed", e);
			c.close("Fetching store node failed: " + e);
		} finally {
			fetch();
		}
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		log.debug("Connection closed, reason '{}' in backend {}", reason, conn);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		// Ignore
	}

}