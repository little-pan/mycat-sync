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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.mysql.nio.handler.CommitNodeHandler;
import org.opencloudb.mysql.nio.handler.KillConnectionHandler;
import org.opencloudb.mysql.nio.handler.MultiNodeCoordinator;
import org.opencloudb.mysql.nio.handler.MultiNodeQueryHandler;
import org.opencloudb.mysql.nio.handler.RollbackNodeHandler;
import org.opencloudb.mysql.nio.handler.RollbackReleaseHandler;
import org.opencloudb.mysql.nio.handler.SingleNodeHandler;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.sqlcmd.SQLCmdConstant;
import org.slf4j.*;

/**
 * @author mycat
 * @author mycat
 */
public class NonBlockingSession implements Session {

    static final Logger log = LoggerFactory.getLogger(NonBlockingSession.class);

	private final ServerConnection source;
	private final ConcurrentHashMap<RouteResultsetNode, BackendConnection> target;
	// life-cycle: each sql execution
	private volatile SingleNodeHandler singleNodeHandler;
	private volatile MultiNodeQueryHandler multiNodeHandler;
	private volatile RollbackNodeHandler rollbackHandler;
	private final MultiNodeCoordinator multiNodeCoordinator;
	private final CommitNodeHandler commitHandler;
	private volatile String xaTXID;

	public NonBlockingSession(ServerConnection source) {
		this.source = source;
		this.target = new ConcurrentHashMap<>(2, 0.75f);
		multiNodeCoordinator = new MultiNodeCoordinator(this);
		commitHandler = new CommitNodeHandler(this);
	}

	@Override
	public ServerConnection getSource() {
		return source;
	}

	@Override
	public int getTargetCount() {
		return target.size();
	}

	public Set<RouteResultsetNode> getTargetKeys() {
		return target.keySet();
	}

	public BackendConnection getTarget(RouteResultsetNode key) {
		return target.get(key);
	}

	public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
		return this.target;
	}

	public BackendConnection removeTarget(RouteResultsetNode key) {
		return target.remove(key);
	}

	@Override
	public void execute(RouteResultset rrs, int type) {
		// clear prev execute resources
		clearHandlesResources();
        log.debug("rrs '{}' in source {}", rrs, this.source);

		// 检查路由结果是否为空
		RouteResultsetNode[] nodes = rrs.getNodes();
		if (nodes == null || nodes.length == 0 || nodes[0].getName() == null
				|| nodes[0].getName().equals("")) {
			source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
					"No dataNode found ,please check tables defined in schema:"
							+ source.getSchema());
			return;
		}
		if (nodes.length == 1) {
			this.singleNodeHandler = new SingleNodeHandler(rrs, this);
			try {
                this.singleNodeHandler.execute();
			} catch (Exception e) {
				log.warn(rrs + " in source " + this.source, e);
				source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
			}
		} else {
			boolean autocommit = source.isAutocommit();
            this.multiNodeHandler = new MultiNodeQueryHandler(type, rrs, autocommit, this);
			try {
                this.multiNodeHandler.execute();
			} catch (Exception e) {
				log.warn(rrs + " in source " + this.source, e);
				source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
			}
		}
	}

	@Override
	public void commit() {
		final int initCount = this.target.size();

		if (initCount <= 0) {
			log.debug("no node in current transaction");
			ByteBuffer buffer = this.source.allocate();
			buffer = this.source.writeToBuffer(OkPacket.OK, buffer);
			this.source.write(buffer);
		} else if (initCount == 1) {
            log.debug("single node in current transaction");
			BackendConnection con = this.target.elements().nextElement();
			this.commitHandler.commit(con);
		} else {
            log.debug("multi node commit to send: node count {}", initCount);
			this.multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);
		}
	}

	public void rollback() {
		final int initCount = this.target.size();
		if (initCount <= 0) {
            log.debug("no session bound connections found, no need send rollback cmd");
			ByteBuffer buffer = this.source.allocate();
			buffer = source.writeToBuffer(OkPacket.OK, buffer);
            this.source.write(buffer);
			return;
		}

        this.rollbackHandler = new RollbackNodeHandler(this);
        this.rollbackHandler.rollback();
	}

	@Override
	public void cancel(FrontendConnection sponsor) {

	}

	/**
	 * {@link ServerConnection#isClosed()} must be true before invoking this
	 */
	public void terminate() {
		for (BackendConnection node : target.values()) {
			node.close("client closed ");
		}
		target.clear();
		clearHandlesResources();
	}

	public void closeAndClearResources(String reason) {
		for (BackendConnection node : target.values()) {
			node.close(reason);
		}
		target.clear();
		clearHandlesResources();
	}

	public void releaseConnectionIfSafe(BackendConnection conn, boolean needRollback) {
		RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();

		if (node != null) {
			if (this.source.isAutocommit() || conn.isFromSlaveDB()
					|| !conn.isModifiedSQLExecuted()) {
				releaseConnection((RouteResultsetNode) conn.getAttachment(), needRollback);
			}
		}
	}

	public void releaseConnection(RouteResultsetNode rrn, final boolean needRollback) {
		BackendConnection c = target.remove(rrn);
		if (c != null) {
            log.debug("release backend {}", c);
			if (c.getAttachment() != null) {
				c.setAttachment(null);
			}
			if (!c.isClosedOrQuit()) {
				if (c.isAutocommit()) {
					c.release();
				} else
              //  if (needRollback)
              {
					c.setResponseHandler(new RollbackReleaseHandler());
					c.rollback();
			}
  //              else {
//					c.release();
//				}
			}
		}
	}

	public void releaseConnections(final boolean needRollback) {
		for (RouteResultsetNode rrn : target.keySet()) {
			releaseConnection(rrn, needRollback);
		}
	}

	public void releaseConnection(BackendConnection con) {
		Iterator<Entry<RouteResultsetNode, BackendConnection>> itor = target
				.entrySet().iterator();
		while (itor.hasNext()) {
			BackendConnection theCon = itor.next().getValue();
			if (theCon == con) {
				itor.remove();
				con.release();
                log.debug("release backend {}", con);
				break;
			}
		}

	}

	/**
	 * @return previous bound connection
	 */
	public BackendConnection bindConnection(RouteResultsetNode key, BackendConnection conn) {
		return target.put(key, conn);
	}

	public boolean tryExistsCon(final BackendConnection conn, RouteResultsetNode node) {
		if (conn == null) {
			return false;
		}
		if (!conn.isFromSlaveDB() || node.canRunnINReadDB(getSource().isAutocommit())) {
            log.debug("found connections in session to use backend {} for {}", conn, node);
			conn.setAttachment(node);
			return true;
		} else {
			// slavedb connection and can't use anymore, release it
            log.debug("release slave connection, can't be used in transaction {} for {}", conn, node);
			releaseConnection(node, false);
		}
		return false;
	}

	protected void kill() {
		boolean hooked = false;
		AtomicInteger count = null;
		Map<RouteResultsetNode, BackendConnection> killees = null;
		for (RouteResultsetNode node : target.keySet()) {
			BackendConnection c = target.get(node);
			if (c != null) {
				if (!hooked) {
					hooked = true;
					killees = new HashMap<>();
					count = new AtomicInteger(0);
				}
				killees.put(node, c);
				count.incrementAndGet();
			}
		}
		if (hooked) {
			for (Entry<RouteResultsetNode, BackendConnection> en : killees
					.entrySet()) {
				KillConnectionHandler kill = new KillConnectionHandler(
						en.getValue(), this);
				MycatConfig conf = MycatServer.getInstance().getConfig();
				PhysicalDBNode dn = conf.getDataNodes().get(
						en.getKey().getName());
				try {
					dn.getConnectionFromSameSource(null,true, en.getValue(),
							kill, en.getKey());
				} catch (Exception e) {
					log.error("get killer connection failed for " + en.getKey(), e);
					kill.connectionError(e, null);
				}
			}
		}
	}

	private void clearHandlesResources() {
		SingleNodeHandler singleHander = singleNodeHandler;
		if (singleHander != null) {
			singleHander.clearResources();
			singleNodeHandler = null;
		}
		MultiNodeQueryHandler multiHandler = multiNodeHandler;
		if (multiHandler != null) {
			multiHandler.clearResources();
			multiNodeHandler = null;
		}
	}

	public void clearResources(final boolean needRollback) {
        log.debug("clear session resources in session {}", this);
		this.releaseConnections(needRollback);
		clearHandlesResources();
	}

	public boolean closed() {
		return source.isClosed();
	}

	private String genXATXID() {
		return MycatServer.getInstance().genXATXID();
	}

	public void setXATXEnabled(boolean xaTXEnabled) {
		log.debug("XA Transaction enabled in source {}", this.getSource());
		if (xaTXEnabled && this.xaTXID == null) {
			xaTXID = genXATXID();
		}
	}

	public String getXaTXID() {
		return xaTXID;
	}

}