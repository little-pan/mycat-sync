package org.opencloudb.mysql.handler;

import org.opencloudb.net.BackendConnection;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerSession;
import org.opencloudb.sqlcmd.SQLCtrlCommand;
import org.slf4j.*;

public class MultiNodeCoordinator extends AbstractResponseHandler {

	static final Logger log = LoggerFactory.getLogger(MultiNodeCoordinator.class);

	// It's thread-safe for single NioProcessor thread execution.
	private int runningCount = 0;
	private int failedCount = 0;
	private volatile int nodeCount;
	private final ServerSession session;
	private SQLCtrlCommand cmdHandler;
	private boolean failed = false;

	public MultiNodeCoordinator(ServerSession session) {
		this.session = session;
	}

	public void executeBatchNodeCmd(SQLCtrlCommand cmdHandler) {
		this.cmdHandler = cmdHandler;
		final int initCount = session.getTargetCount();
		this.runningCount = initCount;
		this.nodeCount = initCount;
		this.failed = false;
		this.failedCount = 0;
		// 执行
		int started = 0;
		for (RouteResultsetNode rrn : session.getTargetKeys()) {
			if (rrn == null) {
				log.error("null is contained in RoutResultsetNodes, source {}", session.getSource());
				continue;
			}
			final BackendConnection conn = session.getTarget(rrn);
			if (conn != null) {
				conn.setResponseHandler(this);
				cmdHandler.sendCommand(session, conn);
				++started;
			}
		}

		if (started < nodeCount) {
			this.runningCount = started;
			log.warn("Some connection failed to execute {}", (nodeCount - started));
			/**
			 * assumption: only caused by front-end connection close. <br/>
			 * Otherwise, packet must be returned to front-end
			 */
			this.failed = true;
		}
	}

	private boolean finished() {
		int val = --this.runningCount;
		return (val == 0);
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		++this.failedCount;

		if (this.cmdHandler.releaseConOnErr()) {
			this.session.releaseConnection(conn);
		} else {
			this.session.releaseConnectionIfSafe(conn);
		}
		if (finished()) {
			this.cmdHandler.errorResponse(session, err, this.nodeCount, this.failedCount);
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (this.cmdHandler.releaseConOnOK()) {
			this.session.releaseConnection(conn);
		} else {
			this.session.releaseConnectionIfSafe(conn);
		}

		if (finished()) {
			this.cmdHandler.okResponse(session, ok);
		}
	}

}
