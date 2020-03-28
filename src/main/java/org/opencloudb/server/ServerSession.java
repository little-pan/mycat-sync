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
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.mysql.handler.CommitNodeHandler;
import org.opencloudb.mysql.handler.MultiNodeCoordinator;
import org.opencloudb.mysql.handler.MultiNodeQueryHandler;
import org.opencloudb.mysql.handler.RollbackNodeHandler;
import org.opencloudb.mysql.handler.SingleNodeHandler;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.net.BackendException;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.sqlcmd.SQLCmdConstant;
import org.slf4j.*;

/**
 * @author mycat
 */
public class ServerSession implements Session {

    static final Logger log = LoggerFactory.getLogger(ServerSession.class);

    // Note: it's thread-safe, session only run in single processor thread.
	private final ServerConnection source;
	private final Map<RouteResultsetNode, BackendConnection> target;
	// life-cycle: each sql execution
	private volatile SingleNodeHandler singleNodeHandler;
	private volatile MultiNodeQueryHandler multiNodeHandler;
	private volatile RollbackNodeHandler rollbackHandler;
	private final MultiNodeCoordinator multiNodeCoordinator;
	private final CommitNodeHandler commitHandler;
	private volatile String xaTXID;

	public ServerSession(ServerConnection source) {
		this.source = source;
		this.target = new HashMap<>(2, 0.75f);
		this.multiNodeCoordinator = new MultiNodeCoordinator(this);
		this.commitHandler = new CommitNodeHandler(this);
	}

	@Override
	public ServerConnection getSource() {
		return source;
	}

	@Override
	public int getTargetCount() {
		return this.target.size();
	}

	public Set<RouteResultsetNode> getTargetKeys() {
		return this.target.keySet();
	}

	public BackendConnection getTarget(RouteResultsetNode key) {
		return target.get(key);
	}

	public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
		return this.target;
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
			this.source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
					"No dataNode found, please check tables defined in schema: "
							+ this.source.getSchema());
			return;
		}

		if (nodes.length == 1) {
			this.singleNodeHandler = new SingleNodeHandler(rrs, this);
			this.singleNodeHandler.execute();
		} else {
            this.multiNodeHandler = new MultiNodeQueryHandler(type, rrs, this);
			this.multiNodeHandler.execute();
		}
	}

	protected BackendConnection singleBackend() {
		Collection<BackendConnection> bc = this.target.values();
		Iterator<BackendConnection> it = bc.iterator();
		if (!it.hasNext() || bc.size() > 1) {
			throw new IllegalStateException("Too many backend connections existing");
		}

		return it.next();
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
			BackendConnection con = singleBackend();
			this.commitHandler.commit(con);
		} else {
            log.debug("multi node commit to send: node count {}", initCount);
			this.multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);
		}
	}

	@Override
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
		for (BackendConnection node: target.values()) {
			node.close("Server session closed");
		}
		this.target.clear();
		clearHandlesResources();
	}

	public void closeAndClearResources(String reason) {
		for (BackendConnection node : this.target.values()) {
			node.close(reason);
		}
		this.target.clear();
		clearHandlesResources();
	}

	public void releaseConnectionIfSafe(BackendConnection conn) {
		RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
		if (this.source.isAutocommit() || conn.isFromSlaveDB() || !conn.isModifiedSQLExecuted()) {
			if (node != null) releaseConnection(node);
		}
	}

	private void releaseConnection(RouteResultsetNode rrn) {
		final BackendConnection bc = this.target.remove(rrn);

		if (bc != null) {
            log.debug("release backend {}", bc);

			bc.setAttachment(null);
			if (bc.isClosedOrQuit()) {
				return;
			}
			// Here only do release, rollback should be trigger by frontend user!
			bc.release();
		}
	}

	public void releaseConnections() {
		for (RouteResultsetNode rrn : this.target.keySet()) {
			releaseConnection(rrn);
		}
	}

	public void releaseConnection(final BackendConnection con) {
		Iterator<Entry<RouteResultsetNode, BackendConnection>> it = target.entrySet().iterator();
		while (it.hasNext()) {
			final BackendConnection c = it.next().getValue();
			if (c == con) {
                it.remove();
				con.release();
				break;
			}
		}
	}

	/**
	 * @return previous bound connection
	 */
	public BackendConnection bindConnection(RouteResultsetNode key, BackendConnection conn) {
		return this.target.put(key, conn);
	}

	public boolean tryExistsCon(final BackendConnection conn, RouteResultsetNode node) {
		if (conn == null) {
			return false;
		}

		if (!conn.isFromSlaveDB() || node.canRunINReadDB(getSource().isAutocommit())) {
            log.debug("found connection in session to use for node '{}': backend {}", node,  conn);
			conn.setAttachment(node);
			return true;
		} else {
			// Slave db connection and can't use anymore, release it
            log.debug("release slave connection, can't be used in transaction for {}: backend {}", node, conn);
			releaseConnection(node);
			return false;
		}
	}

	protected void kill() {
		boolean hooked = false;
		AtomicInteger count = null;
		Map<RouteResultsetNode, BackendConnection> killees = null;
		for (RouteResultsetNode node: target.keySet()) {
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
			throw new BackendException("Kill unsupported");
		}
	}

	private void clearHandlesResources() {
		SingleNodeHandler singleHandler = this.singleNodeHandler;
		if (singleHandler != null) {
			singleHandler.clearResources();
			this.singleNodeHandler = null;
		}
		MultiNodeQueryHandler multiHandler = this.multiNodeHandler;
		if (multiHandler != null) {
			multiHandler.clearResources();
			this.multiNodeHandler = null;
		}
	}

	public void clearResources() {
        log.debug("Clear session resources in session {}", this);
		releaseConnections();
		clearHandlesResources();
	}

	public boolean closed() {
		return this.source.isClosed();
	}

	private String genXATXID() {
		MycatServer server = MycatServer.getContextServer();
		return server.genXATXID();
	}

	public void setXATXEnabled(boolean xaTXEnabled) {
		log.debug("XA Transaction enabled in source {}", this.getSource());
		if (xaTXEnabled && this.xaTXID == null) {
			this.xaTXID = genXATXID();
		}
	}

	public String getXaTXID() {
		return this.xaTXID;
	}

}