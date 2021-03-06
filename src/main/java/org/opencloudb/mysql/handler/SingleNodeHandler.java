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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.mysql.LoadDataUtil;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerSession;
import org.opencloudb.server.ServerConnection;

import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.server.parser.ServerParseShow;
import org.opencloudb.server.response.ShowFullTables;
import org.opencloudb.server.response.ShowTables;

import org.opencloudb.stat.QueryResult;
import org.opencloudb.stat.QueryResultDispatcher;

import org.opencloudb.util.ByteUtil;
import org.opencloudb.util.ExceptionUtil;
import org.opencloudb.util.StringUtil;
import org.slf4j.*;

/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, Terminatable, LoadDataResponseHandler {

	private static final Logger log = LoggerFactory.getLogger(SingleNodeHandler.class);

	private final RouteResultsetNode node;
	private final RouteResultset rrs;
	private final ServerSession session;
	// Single thread access at one time and synchronized, no need lock
	private byte packetId;
	private ByteBuffer buffer;
	private boolean isRunning;
	private Runnable terminateCallBack;
	private long startTime;

    private boolean isDefaultNodeShowTable;
    private boolean isDefaultNodeShowFullTable;
    private Set<String> shardingTablesSet;
	
	public SingleNodeHandler(RouteResultset rrs, ServerSession session) {
		this.rrs = rrs;
		this.node = rrs.getNodes()[0];
		if (this.node == null) {
			throw new IllegalArgumentException("routeNode is null");
		}
		if (session == null) {
			throw new IllegalArgumentException("session is null");
		}
		this.session = session;
        ServerConnection source = session.getSource();
        String schema=source.getSchema();
        if(schema != null && ServerParse.SHOW == rrs.getSqlType()) {
			MycatServer server = MycatServer.getContextServer();
            SchemaConfig schemaConfig= server.getConfig().getSchemas().get(schema);
            int type= ServerParseShow.tableCheck(rrs.getStatement(),0) ;
            boolean noDefaultNode = Strings.isNullOrEmpty(schemaConfig.getDataNode());
            this.isDefaultNodeShowTable = (ServerParseShow.TABLES == type && !noDefaultNode);
            this.isDefaultNodeShowFullTable = (ServerParseShow.FULLTABLES == type && !noDefaultNode);
            if(this.isDefaultNodeShowTable) {
                this.shardingTablesSet = ShowTables.getTableSet(source, rrs.getStatement());
            } else
            if(this.isDefaultNodeShowFullTable) {
                this.shardingTablesSet = ShowFullTables.getTableSet(source, rrs.getStatement());
            }
        }
	}

	@Override
	public void terminate(Runnable callback) {
		boolean zeroReached = false;

		if (isRunning) {
			terminateCallBack = callback;
		} else {
			zeroReached = true;
		}

		if (zeroReached) {
			callback.run();
		}
	}

	private void endRunning() {
		Runnable callback = null;
		if (this.isRunning) {
			this.isRunning = false;
			callback = this.terminateCallBack;
			this.terminateCallBack = null;
		}

		if (callback != null) {
			callback.run();
		}
	}

	private void recycleResources() {
		ByteBuffer buf = this.buffer;
		if (buf != null) {
			this.buffer = null;
			this.session.getSource().recycle(this.buffer);
		}
	}

	public void execute() {
	    log.debug("execute SQL in node '{}'", this.node);

		this.startTime = System.currentTimeMillis();
		this.isRunning = true;
		this.packetId = 0;

		final BackendConnection conn = this.session.getTarget(this.node);
		if (this.session.tryExistsCon(conn, this.node)) {
			executeOn(conn);
		} else {
			ServerConnection source = this.session.getSource();
			// create new connection
			MycatServer server = MycatServer.getContextServer();
			MycatConfig conf = server.getConfig();
			PhysicalDBNode dn = conf.getDataNodes().get(this.node.getName());
			if (dn == null) {
				endRunning();
				String charset = source.getCharset(), node = this.node.getName();
				String errmsg = "Unknown datanode '"+ node +"'";
				ErrorPacket err = ErrorPacket.create(++this.packetId,
						ErrorCode.ER_WRONG_ARGUMENTS, errmsg, charset);
				err.write(source);
				return;
			}

			String db = dn.getDatabase();
			boolean ac = source.isAutocommit();
			dn.getConnection(db, ac, this.node, this, this.node);
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
	    log.debug("acquire a backend {} in node {}, and bind it into session", conn, this.node);
		this.session.bindConnection(this.node, conn);
		executeOn(conn);
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		String charset = this.session.getSource().getCharset();
		ErrorPacket err = ErrorPacket.create(++this.packetId,
				ErrorCode.ER_ERROR_ON_CLOSE, reason, charset);
		backConnectionErr(err, conn);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		endRunning();
		String charset = this.session.getSource().getCharset();
		ErrorPacket err = ErrorPacket.create(++this.packetId,
				ErrorCode.ER_NEW_ABORTING_CONNECTION, e.getMessage(), charset);
		ServerConnection source = session.getSource();
		source.write(err.write(allocBuffer(), source, true));
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		err.packetId = ++this.packetId;
		backConnectionErr(err, conn);
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {        
		boolean executed = conn.syncAndExecute();
		if (executed) {
			this.session.releaseConnectionIfSafe(conn);
			endRunning();
			ServerConnection source = this.session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
			if (this.rrs.isLoadData()) {
				byte lastPackId = source.getLoadDataInfileHandler().getLastPackId();
				ok.packetId = ++lastPackId; // OK_PACKET
				source.getLoadDataInfileHandler().clear();
			} else {
				ok.packetId = ++this.packetId;// OK_PACKET
			}
			ok.serverStatus = source.isAutocommit() ? 2 : 1;

			recycleResources();
			source.setLastInsertId(ok.insertId);
			ok.write(source);
			
			// add by zhuam
			//查询结果派发
			QueryResult queryResult = new QueryResult(this.session.getSource().getUser(),
					rrs.getSqlType(), rrs.getStatement(), startTime, System.currentTimeMillis());
			QueryResultDispatcher.dispatchQuery(queryResult);
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
	    log.debug("query eof in backend {}", conn);
		ServerConnection source = this.session.getSource();
		conn.recordSql(source.getHost(), source.getSchema(), this.node.getStatement());

		// 判断是调用存储过程的话不能在这里释放连接
		if (!this.rrs.isCallStatement()) {
            this.session.releaseConnectionIfSafe(conn);
			endRunning();
		}

		eof[3] = ++this.packetId;
		this.buffer = source.writeToBuffer(eof, allocBuffer());
		source.write(this.buffer);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
								 byte[] eof, BackendConnection conn) {

		// add by zhuam
		//查询结果派发
		QueryResult queryResult = new QueryResult(session.getSource().getUser(),
				rrs.getSqlType(), rrs.getStatement(), startTime, System.currentTimeMillis());
		QueryResultDispatcher.dispatchQuery( queryResult );

		header[3] = ++packetId;
		ServerConnection source = session.getSource();
		buffer = source.writeToBuffer(header, allocBuffer());
		for (int i = 0, len = fields.size(); i < len; ++i)
		{
			byte[] field = fields.get(i);
			field[3] = ++packetId;
			buffer = source.writeToBuffer(field, buffer);
		}
		eof[3] = ++packetId;
		buffer = source.writeToBuffer(eof, buffer);

		if (isDefaultNodeShowTable) {
			for (String name : shardingTablesSet) {
				RowDataPacket row = new RowDataPacket(1);
				row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, source, true);
			}
		}  else
		if (isDefaultNodeShowFullTable) {
			for (String name : shardingTablesSet) {
				RowDataPacket row = new RowDataPacket(1);
				row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
				row.add(StringUtil.encode("BASE TABLE", source.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, source, true);
			}
		}
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		if(this.isDefaultNodeShowTable || this.isDefaultNodeShowFullTable) {
			RowDataPacket rowDataPacket =new RowDataPacket(1);
			rowDataPacket.read(row);
			String table=  StringUtil.decode(rowDataPacket.fieldValues.get(0),conn.getCharset());
			if(this.shardingTablesSet.contains(table.toUpperCase())) return;
		}

		row[3] = ++this.packetId;
		ServerConnection source = this.session.getSource();
		this.buffer = source.writeToBuffer(row, allocBuffer());
	}

	private void executeOn(BackendConnection conn) {
		if (this.session.closed()) {
			endRunning();
			this.session.clearResources();
			return;
		}

		conn.setResponseHandler(this);
		try {
			ServerConnection sc = this.session.getSource();
			conn.execute(this.node, sc, sc.isAutocommit());
		} catch (Exception e) {
			executeException(conn, e);
		}
	}

	private void executeException(BackendConnection c, Exception e) {
		String charset = session.getSource().getCharset();
		String errmsg = ExceptionUtil.getClientMessage(e);
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ERR_FOUND_EXCEPION;
		err.message = StringUtil.encode(errmsg, charset);
		backConnectionErr(err, c);
	}

	private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn) {
		endRunning();

		ServerConnection source = session.getSource();
		String errUser = source.getUser();
		String errHost = source.getHost();
		int errPort = source.getLocalPort();

		String errmsg = "Error: errno " + errPkg.errno
				+ ", errmsg '" + new String(errPkg.message) + "'";
		log.warn("Execute sql error: '{}', frontend host: '{}:{}/{}', con: {}",
				errmsg, errHost, errPort, errUser, conn);
		this.session.releaseConnectionIfSafe(conn);

		source.setTxInterrupt(errmsg);
		errPkg.write(source);
		recycleResources();
	}

	/**
	 * lazy create ByteBuffer only when needed
	 * 
	 * @return
	 */
	private ByteBuffer allocBuffer() {
		if (buffer == null) {
			buffer = session.getSource().allocate();
		}
		return buffer;
	}

	public void clearResources() {

	}

	@Override
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		if (log.isDebugEnabled()) {
			log.debug("request data response: {}", ByteUtil.dump(data));
		}
		LoadDataUtil.requestFileDataResponse(data, conn);
	}

	@Override
	public String toString() {
		return "SingleNodeHandler [node=" + node + ", packetId=" + packetId + "]";
	}

}
