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
package org.opencloudb.server;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.NioProcessor;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.server.handler.MysqlProcHandler;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.server.response.Heartbeat;
import org.opencloudb.server.response.Ping;
import org.opencloudb.server.util.SchemaUtil;
import org.opencloudb.util.TimeUtil;
import org.slf4j.*;

/**
 * @author mycat
 */
public class ServerConnection extends FrontendConnection {

	private static final Logger log = LoggerFactory.getLogger(ServerConnection.class);

	private static final long AUTH_TIMEOUT = 15 * 1000L;

	private volatile int txIsolation;
	private volatile boolean autocommit;
	private volatile boolean txInterrupted;
	private volatile String txInterrputMsg = "";
	private long lastInsertId;
	private ServerSession session;

	public ServerConnection(SocketChannel channel) throws IOException {
		super(channel);
		this.txInterrupted = false;
		this.autocommit = true;
	}

	@Override
	public boolean isIdleTimeout() {
		if (this.isAuthenticated) {
			return super.isIdleTimeout();
		} else {
			return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
					lastReadTime) + AUTH_TIMEOUT;
		}
	}

	public int getTxIsolation() {
		return txIsolation;
	}

	public void setTxIsolation(int txIsolation) {
		this.txIsolation = txIsolation;
	}

	public boolean isAutocommit() {
		return autocommit;
	}

	public void setAutocommit(boolean autocommit) {
		this.autocommit = autocommit;
	}

	public long getLastInsertId() {
		return lastInsertId;
	}

	public void setLastInsertId(long lastInsertId) {
		this.lastInsertId = lastInsertId;
	}

	/**
	 * 设置是否需要中断当前事务
	 */
	public void setTxInterrupt(String txInterruptMsg) {
		if (!this.autocommit && !this.txInterrupted) {
			this.txInterrupted = true;
			this.txInterrputMsg = txInterruptMsg;
		}
	}

	public boolean isTxInterrupted()
	{
		return this.txInterrupted;
	}

	public ServerSession getSession() {
		return this.session;
	}

	public void setSession(ServerSession session) {
		this.session = session;
	}

	@Override
	public void ping() {
		Ping.response(this);
	}

	@Override
	public void heartbeat(byte[] data) {
		Heartbeat.response(this, data);
	}

	public void execute(String sql, int type) {
		if (isClosed()) {
			log.warn("Ignore execute, server connection is closed: conn {}", this);
			return;
		}

		if (this.txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback: " + this.txInterrputMsg);
			return;
		}

		String db = this.schema;
		if (db == null) {
            db = SchemaUtil.detectDefaultDb(sql, type);
            if(db == null) {
                writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,"No MyCAT Database selected");
                return;
            }
		}

        if(ServerParse.SELECT == type && sql.contains("mysql") && sql.contains("proc")) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if(schemaInfo!=null&&"mysql".equalsIgnoreCase(schemaInfo.schema)&&"proc".equalsIgnoreCase(schemaInfo.table))
            {
                //兼容MySQLWorkbench
                MysqlProcHandler.handle(sql,this);
                return;
            }
        }
		MycatServer server = MycatServer.getContextServer();
		SchemaConfig schema = server.getConfig().getSchemas().get(db);
		if (schema == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"Unknown MyCAT Database '" + db + "'");
			return;
		}

		routeEndExecuteSQL(sql, type, schema);
	}

    public RouteResultset routeSQL(String sql, int type) {
		// 检查当前使用的DB
		String db = this.schema;
		if (db == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"No MyCAT Database selected");
			return null;
		}
		MycatServer server = MycatServer.getContextServer();
		SchemaConfig schema = server.getConfig().getSchemas().get(db);
		if (schema == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"Unknown MyCAT Database '" + db + "'");
			return null;
		}

		// 路由计算
		RouteResultset rrs;
		try {
			rrs = server.getRouterservice()
					.route(server.getConfig().getSystem(),
							schema, type, sql, this.charset, this);
		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			log.warn(s.append(this).append(sql).toString() + " error:" + e.toString(), e);
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
			return null;
		}

		return rrs;
	}

	public void routeEndExecuteSQL(String sql, int type, SchemaConfig schema) {
		// 路由计算
		RouteResultset rrs;
		try {
			MycatServer server = MycatServer.getContextServer();
			rrs = server.getRouterservice()
					.route(server.getConfig().getSystem(),
							schema, type, sql, this.charset, this);
		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			log.warn(s.append(this).append(sql).toString() + " error:" + e.toString(), e);
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
			return;
		}
		if (rrs != null) {
			// session执行
			this.session.execute(rrs, type);
		}
	}

	public void commit() {
		if (this.txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES, "Transaction status error, need to rollback.");
		} else {
			this.session.commit();
		}
	}

	public void rollback() {
		if (this.txInterrupted) {
			this.txInterrupted = false;
		}

        this.session.rollback();
	}

	/**
	 * 撤销执行中的语句
	 * 
	 * @param sponsor
	 *            发起者为null表示是自己
	 */
	public void cancel(final FrontendConnection sponsor) {
		Runnable cancelTask = new Runnable() {
			@Override
			public void run() {
				session.cancel(sponsor);
			}
		};
		NioProcessor.currentProcessor().execute(cancelTask);
	}

	@Override
	protected void doClose(String reason) {
		if (!isClosed()) {
			super.doClose(reason);
			this.session.terminate();
			if(this.loadDataInfileHandler != null) {
				this.loadDataInfileHandler.clear();
				this.loadDataInfileHandler = null;
			}
		}
	}

	@Override
	public String toString() {
		return "ServerConnection [id=" + id + ", schema=" + schema + ", host="
				+ host + ", user=" + user + ", txIsolation=" + txIsolation
				+ ", autocommit=" + autocommit + ", schema=" + schema + "]";
	}

}