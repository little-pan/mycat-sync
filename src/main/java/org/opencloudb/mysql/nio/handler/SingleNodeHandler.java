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
package org.opencloudb.mysql.nio.handler;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.mysql.LoadDataUtil;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;

import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.server.parser.ServerParseShow;
import org.opencloudb.server.response.ShowFullTables;
import org.opencloudb.server.response.ShowTables;

import org.opencloudb.stat.QueryResult;
import org.opencloudb.stat.QueryResultDispatcher;

import org.opencloudb.util.StringUtil;
import org.slf4j.*;

/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, Terminatable, LoadDataResponseHandler {

	private static final Logger log = LoggerFactory.getLogger(SingleNodeHandler.class);

	private final RouteResultsetNode node;
	private final RouteResultset rrs;
	private final NonBlockingSession session;
	// only one thread access at one time no need lock
	private volatile byte packetId;
	private volatile ByteBuffer buffer;
	private volatile boolean isRunning;
	private Runnable terminateCallBack;
	private long startTime;

    private volatile boolean isDefaultNodeShowTable;
    private volatile boolean isDefaultNodeShowFullTable;
    private  Set<String> shardingTablesSet;
	
	public SingleNodeHandler(RouteResultset rrs, NonBlockingSession session) {
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
            SchemaConfig schemaConfig= MycatServer.getInstance().getConfig().getSchemas().get(schema);
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
		if (isRunning) {
			isRunning = false;
			callback = terminateCallBack;
			terminateCallBack = null;
		}

		if (callback != null) {
			callback.run();
		}
	}

	private void recycleResources() {
		ByteBuffer buf = buffer;
		if (buf != null) {
			buffer = null;
			session.getSource().recycle(buffer);
		}
	}

	public void execute() throws Exception {
	    log.debug("execute SQL in node {}", this.node);
		this.startTime = System.currentTimeMillis();
		ServerConnection sc = this.session.getSource();
		this.isRunning = true;
		this.packetId = 0;
		final BackendConnection conn = this.session.getTarget(this.node);
		if (this.session.tryExistsCon(conn, this.node)) {
			_execute(conn);
		} else {
			// create new connection
			MycatConfig conf = MycatServer.getInstance().getConfig();
			PhysicalDBNode dn = conf.getDataNodes().get(this.node.getName());
			dn.getConnection(dn.getDatabase(), sc.isAutocommit(), this.node, this, this.node);
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
	    log.debug("acquire a backend {} in node {}, and bind it into session", conn, this.node);
		this.session.bindConnection(this.node, conn);
		_execute(conn);
	}

	private void _execute(BackendConnection conn) {
		if (this.session.closed()) {
			endRunning();
            this.session.clearResources(true);
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
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ERR_FOUND_EXCEPION;
		err.message = StringUtil.encode(e.toString(), session.getSource().getCharset());

		this.backConnectionErr(err, c);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		endRunning();
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_NEW_ABORTING_CONNECTION;
		err.message = StringUtil.encode(e.getMessage(), session.getSource()
				.getCharset());
		ServerConnection source = session.getSource();
		source.write(err.write(allocBuffer(), source, true));
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		err.packetId = ++packetId;
		backConnectionErr(err, conn);
	}

	private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn) {
		endRunning();
		
		ServerConnection source = session.getSource();
		String errUser = source.getUser();
		String errHost = source.getHost();
		int errPort = source.getLocalPort();
		
		String errmgs = " errno:" + errPkg.errno + " " + new String(errPkg.message);
		log.warn("Execute sql error: '{}', frontend host: '{}:{}/{}', con: {}", errmgs, errHost, errPort, errUser, conn);
		session.releaseConnectionIfSafe(conn, false);
		
		source.setTxInterrupt(errmgs);
		errPkg.write(source);
		recycleResources();
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {        
		boolean executeResponse = conn.syncAndExcute();		
		if (executeResponse) {			
			session.releaseConnectionIfSafe(conn, false);
			endRunning();
			ServerConnection source = session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
			if (rrs.isLoadData()) {
				byte lastPackId = source.getLoadDataInfileHandler()
						.getLastPackId();
				ok.packetId = ++lastPackId;// OK_PACKET
				source.getLoadDataInfileHandler().clear();
			} else {
				ok.packetId = ++packetId;// OK_PACKET
			}
			ok.serverStatus = source.isAutocommit() ? 2 : 1;

			recycleResources();
			source.setLastInsertId(ok.insertId);
			ok.write(source);
			
			// TODO: add by zhuam
			//查询结果派发
			QueryResult queryResult = new QueryResult(session.getSource().getUser(), 
					rrs.getSqlType(), rrs.getStatement(), startTime, System.currentTimeMillis());
			QueryResultDispatcher.dispatchQuery( queryResult );
 
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
	    log.debug("query eof in backend {}", conn);
		ServerConnection source = this.session.getSource();
		conn.recordSql(source.getHost(), source.getSchema(), this.node.getStatement());

		// 判断是调用存储过程的话不能在这里释放连接
		if (!this.rrs.isCallStatement()) {
            this.session.releaseConnectionIfSafe(conn, false);
			endRunning();
		}

		eof[3] = ++this.packetId;
		this.buffer = source.writeToBuffer(eof, allocBuffer());
		source.write(this.buffer);
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

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {

			//TODO: add by zhuam
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
        if(isDefaultNodeShowTable||isDefaultNodeShowFullTable) {
            RowDataPacket rowDataPacket =new RowDataPacket(1);
            rowDataPacket.read(row);
            String table=  StringUtil.decode(rowDataPacket.fieldValues.get(0),conn.getCharset());
            if(shardingTablesSet.contains(table.toUpperCase())) return;
        }

        row[3] = ++packetId;
        buffer = session.getSource().writeToBuffer(row, allocBuffer());
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_ERROR_ON_CLOSE;
		err.message = StringUtil.encode(reason, session.getSource().getCharset());
		this.backConnectionErr(err, conn);
	}

	public void clearResources() {

	}

	@Override
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		LoadDataUtil.requestFileDataResponse(data, conn);
	}

	@Override
	public String toString() {
		return "SingleNodeHandler [node=" + node + ", packetId=" + packetId
				+ "]";
	}

}