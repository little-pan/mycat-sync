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

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.mpp.ColMeta;
import org.opencloudb.mpp.DataMergeService;
import org.opencloudb.mpp.MergeCol;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.net.mysql.*;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerSession;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.stat.QueryResult;
import org.opencloudb.stat.QueryResultDispatcher;
import org.slf4j.*;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements LoadDataResponseHandler {

	private static final Logger log = LoggerFactory.getLogger(MultiNodeQueryHandler.class);

	private final RouteResultset rrs;
	private final ServerSession session;
	private final DataMergeService dataMergeSvr;
	private final boolean autocommit;

	private String primaryKeyTable = null;
	private int primaryKeyIndex = -1;
	private int fieldCount = 0;
	private long affectedRows;
	private long insertId;
	private boolean fieldsReturned;
	private int okCount;
	private final boolean isCallProcedure;
	private long startTime;
	private int execCount = 0;

	public MultiNodeQueryHandler(int sqlType, RouteResultset rrs, ServerSession session) {
		super(session);
		if (rrs.getNodes() == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		log.debug("Execute multi-node query '{}'", rrs.getStatement());

		this.rrs = rrs;
		if (ServerParse.SELECT == sqlType && rrs.needMerge()) {
			this.dataMergeSvr = new DataMergeService(this, rrs);
			log.debug("Has data merge logic");
		} else {
			this.dataMergeSvr = null;
		}
		this.isCallProcedure = rrs.isCallStatement();
		this.autocommit = session.getSource().isAutocommit();
		this.session = session;
	}

	protected void reset(int initCount) {
		super.reset(initCount);
		this.okCount = initCount;
		this.execCount = 0;
	}

	public ServerSession getSession() {
		return this.session;
	}

	public void execute() {
		MycatServer server = MycatServer.getContextServer();
		MycatConfig conf = server.getConfig();

		this.reset(rrs.getNodes().length);
		this.fieldsReturned = false;
		this.affectedRows = 0L;
		this.insertId = 0L;
		this.startTime = System.currentTimeMillis();
		for (final RouteResultsetNode node : this.rrs.getNodes()) {
			final BackendConnection conn = this.session.getTarget(node);
			if (this.session.tryExistsCon(conn, node)) {
				executeOn(conn, node);
			} else {
				// Acquire a new connection
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				dn.getConnection(dn.getDatabase(), this.autocommit, node, this, node);
			}
		}
	}

	private void executeOn(BackendConnection conn, RouteResultsetNode node) {
		if (clearIfSessionClosed(this.session)) {
			return;
		}

		ServerConnection source = this.session.getSource();
		conn.setResponseHandler(this);
		conn.execute(node, source, this.autocommit);
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
		this.session.bindConnection(node, conn);
		executeOn(conn, node);
	}

	private boolean decrementOkCount() {
		return (--this.okCount == 0);
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExecute();
		log.debug("received ok response, executeResponse: {} from backend {}", executeResponse, conn);
		if (executeResponse) {
			if (clearIfSessionClosed(this.session)) {
				return;
			}
			if (canClose(conn, isFail())) {
				return;
			}

			ServerConnection source = this.session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
			// 判断是否是全局表，如果是，执行行数不做累加，以最后一次执行的为准。
			if (!this.rrs.isGlobalTable()) {
				this.affectedRows += ok.affectedRows;
			} else {
				this.affectedRows = ok.affectedRows;
			}
			if (ok.insertId > 0) {
				this.insertId = (insertId == 0) ? ok.insertId: Math.min(insertId, ok.insertId);
			}

			// 对于存储过程，其比较特殊，查询结果返回EndRow报文以后，还会再返回一个OK报文，才算结束
			final boolean isEndPacket = this.isCallProcedure ? decrementOkCount()
					: decrementCountBy(1);
			if (isEndPacket) {
				if (this.autocommit) {// clear all connections
					this.session.releaseConnections();
				}
				if (isFail() || this.session.closed()) {
					tryErrorFinished(true);
					return;
				}

				try {
					if (this.rrs.isLoadData()) {
						byte lastPackId = source.getLoadDataInfileHandler().getLastPackId();
						ok.packetId = ++lastPackId;// OK_PACKET
						ok.message = ("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0")
								.getBytes(); // 此处信息只是为了控制台给人看的
						source.getLoadDataInfileHandler().clear();
					} else {
						ok.packetId = ++packetId;// OK_PACKET
					}

					ok.affectedRows = affectedRows;
					ok.serverStatus = source.isAutocommit() ? 2 : 1;
					if (insertId > 0) {
						ok.insertId = insertId;
						source.setLastInsertId(insertId);
					}
					ok.write(source);
				} catch (Exception | OutOfMemoryError e) {
					handleDataProcessFatal(e);
				}
			}
		}
	}

	@Override
	public void rowEofResponse(final byte[] eof, BackendConnection conn) {
		log.debug("on row end response in backend {} ", conn);
		if (this.errorResponsed) {
			conn.close(this.error);
			return;
		}

		final ServerConnection source = this.session.getSource();
		if (!this.isCallProcedure) {
			if (clearIfSessionClosed(this.session)) {
				return;
			} else if (canClose(conn, false)) {
				return;
			}
		}

		if (decrementCountBy(1)) {
			if (!this.isCallProcedure) {
				if (this.autocommit) {// clear all connections
					this.session.releaseConnections();
				}
				if (this.isFail() || session.closed()) {
					tryErrorFinished(true);
					return;
				}
			}
			if (this.dataMergeSvr != null) {
				try {
					this.dataMergeSvr.outputMergeResult(this.session, eof);
					log.debug("Multi-nq: signal output merge result");
					this.dataMergeSvr.run();
				} catch (Exception | OutOfMemoryError e) {
					handleDataProcessFatal(e);
				}
			} else {
				eof[3] = ++this.packetId;
				log.debug("last packet id: {}", this.packetId);
				source.write(eof);
			}
		}
	}

	public void outputMergeResult(final ServerConnection source,
			final byte[] eof, List<RowDataPacket> results) {
		try {
			ByteBuffer buffer = this.session.getSource().allocate();
			final RouteResultset rrs = this.dataMergeSvr.getRrs();

			// 处理limit语句
			int start = rrs.getLimitStart();
			int end = start + rrs.getLimitSize();

         	if (start < 0) {
				start = 0;
			}
			if (rrs.getLimitSize() < 0) {
				end = results.size();
			}

			if (end > results.size()) {
				end = results.size();
			}

			for (int i = start; i < end; i++) {
				RowDataPacket row = results.get(i);
				row.packetId = ++this.packetId;
				buffer = row.write(buffer, source, true);
			}

			eof[3] = ++this.packetId;
			log.debug("last packet id: {}", this.packetId);
			source.write(source.writeToBuffer(eof, buffer));
		} catch (Exception | OutOfMemoryError e) {
			handleDataProcessFatal(e);
		} finally {
			clearResources();
		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
		this.execCount++;
		if (this.execCount == this.rrs.getNodes().length) {
			// add by zhuam
			//查询结果派发
			QueryResult queryResult = new QueryResult(this.session.getSource().getUser(),
					this.rrs.getSqlType(), this.rrs.getStatement(), this.startTime, System.currentTimeMillis());
			QueryResultDispatcher.dispatchQuery( queryResult);
		}
		if (this.fieldsReturned) {
			return;
		}
		this.fieldsReturned = true;

		try {
			boolean needMerge = (this.dataMergeSvr != null) && this.dataMergeSvr.getRrs().needMerge();
			Set<String> shouldRemoveAvgField = new HashSet<>();
			Set<String> shouldRenameAvgField = new HashSet<>();
			if (needMerge) {
				Map<String, Integer> mergeColsMap = this.dataMergeSvr.getRrs().getMergeCols();
				if (mergeColsMap != null) {
					for (Map.Entry<String, Integer> entry : mergeColsMap.entrySet()) {
						String key = entry.getKey();
						int mergeType = entry.getValue();
						if (MergeCol.MERGE_AVG == mergeType && mergeColsMap.containsKey(key + "SUM")) {
							shouldRemoveAvgField.add((key + "COUNT").toUpperCase());
							shouldRenameAvgField.add((key + "SUM").toUpperCase());
						}
					}
				}
			}

			ServerConnection source = this.session.getSource();
			ByteBuffer buffer = source.allocate();
			this.fieldCount = fields.size();
			if (shouldRemoveAvgField.size() > 0) {
				ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
				packet.packetId = ++this.packetId;
				packet.fieldCount = this.fieldCount - shouldRemoveAvgField.size();
				buffer = packet.write(buffer, source, true);
			} else {
				header[3] = ++this.packetId;
				buffer = source.writeToBuffer(header, buffer);
			}

			String primaryKey = null;
			if (this.rrs.hasPrimaryKeyToCache()) {
				String[] items = this.rrs.getPrimaryKeyItems();
				this.primaryKeyTable = items[0];
				primaryKey = items[1];
			}

			Map<String, ColMeta> columnToIndex = new HashMap<>(this.fieldCount);
			for (int i = 0, len = this.fieldCount; i < len; ++i) {
				boolean shouldSkip = false;
				byte[] field = fields.get(i);
				if (needMerge) {
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					String fieldName = new String(fieldPkg.name).toUpperCase();
					if (!columnToIndex.containsKey(fieldName)) {
						if (shouldRemoveAvgField.contains(fieldName)) {
							shouldSkip = true;
						}
						if (shouldRenameAvgField.contains(fieldName)) {
							String newFieldName = fieldName.substring(0, fieldName.length() - 3);
							fieldPkg.name = newFieldName.getBytes();
							fieldPkg.packetId = ++this.packetId;
							shouldSkip = true;
							buffer = fieldPkg.write(buffer, source, false);
						}
						columnToIndex.put(fieldName, new ColMeta(i, fieldPkg.type));
					}
				} else if (primaryKey != null && this.primaryKeyIndex == -1) {
					// find primary key index
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					String fieldName = new String(fieldPkg.name);
					if (primaryKey.equalsIgnoreCase(fieldName)) {
						this.primaryKeyIndex = i;
						this.fieldCount = fields.size();
					}
				}
				if (!shouldSkip) {
					field[3] = ++this.packetId;
					buffer = source.writeToBuffer(field, buffer);
				}
			}
			eof[3] = ++this.packetId;
			buffer = source.writeToBuffer(eof, buffer);
			source.write(buffer);
			if (this.dataMergeSvr != null) {
				this.dataMergeSvr.onRowMetaData(columnToIndex, this.fieldCount);
			}
		} catch (Exception | OutOfMemoryError e) {
			handleDataProcessFatal(e);
		}
	}

	private void handleDataProcessFatal(Throwable cause) {
		if (!this.errorResponsed) {
			// Release resources
			clearResources();
			log.warn("Caught exception", cause);
			setFail(cause.toString());
			tryErrorFinished(true);
		}
	}

	@Override
	public void rowResponse(final byte[] row, final BackendConnection conn) {
		if (this.errorResponsed) {
			conn.close(this.error);
			return;
		}

		try {
			RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
			String dataNode = rNode.getName();
			if (this.dataMergeSvr != null) {
				if (this.dataMergeSvr.onNewRecord(dataNode, row)) {
					this.isClosedByDiscard = true;
					log.debug("Multi-nq: signal closed by discard");
					this.dataMergeSvr.run();
				} else if (this.dataMergeSvr.canRun()) {
					log.debug("Multi-nq: signal batch rows reached");
					this.dataMergeSvr.run();
				}
			} else {
				// cache primaryKey-> dataNode
				if (this.primaryKeyIndex != -1) {
					RowDataPacket rowDataPkg = new RowDataPacket(this.fieldCount);
					rowDataPkg.read(row);
					String primaryKey = new String(rowDataPkg.fieldValues.get(this.primaryKeyIndex));
					MycatServer server = MycatServer.getContextServer();
					LayerCachePool pool = server.getRouterservice().getTableId2DataNodeCache();
					pool.putIfAbsent(this.primaryKeyTable, primaryKey, dataNode);
				}
				row[3] = ++this.packetId;
				this.session.getSource().write(row);
			}
		} catch (Exception | OutOfMemoryError e) {
			handleDataProcessFatal(e);
		}
	}

	@Override
	public void clearResources() {
		if (this.dataMergeSvr != null) {
			this.dataMergeSvr.clear();
		}
	}

	@Override
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		throw new UnsupportedOperationException();
	}

}
