package org.opencloudb.mysql.handler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerSession;
import org.opencloudb.sqlcmd.SQLCtrlCommand;

public class MultiNodeCoordinator extends AbstractResponseHandler {
	private static final Logger LOGGER = Logger.getLogger(MultiNodeCoordinator.class);

	private final AtomicInteger runningCount = new AtomicInteger(0);
	private final AtomicInteger failedCount = new AtomicInteger(0);
	private volatile int nodeCount;
	private final ServerSession session;
	private SQLCtrlCommand cmdHandler;
	private final AtomicBoolean failed = new AtomicBoolean(false);

	public MultiNodeCoordinator(ServerSession session) {
		this.session = session;
	}

	public void executeBatchNodeCmd(SQLCtrlCommand cmdHandler) {
		this.cmdHandler = cmdHandler;
		final int initCount = session.getTargetCount();
		runningCount.set(initCount);
		nodeCount = initCount;
		failed.set(false);
		failedCount.set(0);
		// 执行
		int started = 0;
		for (RouteResultsetNode rrn : session.getTargetKeys()) {
			if (rrn == null) {
				LOGGER.error("null is contained in RoutResultsetNodes, source = "
						+ session.getSource());
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
			runningCount.set(started);
			LOGGER.warn("some connection failed to execut "
					+ (nodeCount - started));
			/**
			 * assumption: only caused by front-end connection close. <br/>
			 * Otherwise, packet must be returned to front-end
			 */
			failed.set(true);
		}
	}

	private boolean finished() {
		int val = runningCount.decrementAndGet();
		return (val == 0);
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		this.failedCount.incrementAndGet();

		if (this.cmdHandler.releaseConOnErr()) {
			this.session.releaseConnection(conn);
		} else {
			this.session.releaseConnectionIfSafe(conn);
		}
		if (finished()) {
			this.cmdHandler.errorResponse(session, err, this.nodeCount, this.failedCount.get());
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
