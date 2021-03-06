package org.opencloudb.sqlengine;

import java.util.List;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.mysql.handler.AbstractResponseHandler;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDataSource;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;
import org.slf4j.*;

/**
 * Async execute in EngineCtx or standalone (EngineCtx = null)
 * 
 * @author wuzhih
 * 
 */
public class SQLJob extends AbstractResponseHandler implements Runnable {

	public static final Logger log = LoggerFactory.getLogger(SQLJob.class);

	private final String sql;
	private final String dataNodeOrDatabase;
	private final SQLJobHandler jobHandler;
	private final EngineCtx ctx;
	private final PhysicalDataSource ds;
	private final int id;

	private BackendConnection connection;
	private volatile boolean finished;

	public SQLJob(int id, String sql, String dataNode, SQLJobHandler jobHandler, EngineCtx ctx) {
		this.id = id;
		this.sql = sql;
		this.dataNodeOrDatabase = dataNode;
		this.jobHandler = jobHandler;
		this.ctx = ctx;
		this.ds = null;
	}

	public SQLJob(String sql, String databaseName, SQLJobHandler jobHandler, PhysicalDataSource ds) {
		this.id = 0;
		this.sql = sql;
		this.dataNodeOrDatabase = databaseName;
		this.jobHandler = jobHandler;
		this.ctx = null;
		this.ds = ds;

	}

	public void run() {
		try {
			if (this.ds == null) {
				RouteResultsetNode node = new RouteResultsetNode(this.dataNodeOrDatabase, ServerParse.SELECT, this.sql);
				// create new connection
				MycatServer server = MycatServer.getContextServer();
				MycatConfig conf = server.getConfig();
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				dn.getConnection(dn.getDatabase(), true, node, this, node);
			} else {
				this.ds.getConnection(this.dataNodeOrDatabase, true, null, this, null);
			}
		} catch (Exception e) {
			log.warn("can't get connection for sql job", e);
			doFinished(true);
		}
	}

	public void terminate(String reason) {
		log.debug("Terminate job reason: {}, con: {}, sql: '{}'", reason, this.connection, this.sql);
		if (this.connection != null) {
			this.connection.close(reason);
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		log.debug("Query sql '{}' in backend {}", this.sql, conn);
		try {
			this.connection = conn;
			conn.setResponseHandler(this);
			conn.query(sql);
		} catch (Throwable cause) {
			try {
				log.warn("Execute sql job failed", cause);
				conn.close("Execute sql job failed: " + cause);
			} finally {
				doFinished(true);
			}
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		if (log.isWarnEnabled()) {
			log.warn("Can't get connection for sql '" + sql + "'", e);
		}

		try {
			if (conn != null) {
				conn.close("Connection failed: " + e);
			}
		} finally {
			doFinished(true);
		}
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		ErrorPacket errPg = new ErrorPacket();
		errPg.read(err);
		conn.release();
		doFinished(true);

		if (log.isDebugEnabled()) {
			log.debug("Error response {} from of sql '{}' at con {}",
							new String(errPg.message), this.sql, conn);
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		log.debug("ok response in {}", conn);
		conn.syncAndExecute();
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		this.jobHandler.onHeader(dataNodeOrDatabase, header, fields);
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		boolean finished = jobHandler.onRowData(dataNodeOrDatabase, row);
		if (finished) {
			conn.close("not needed by user process");
			doFinished(false);
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		conn.release();
		doFinished(false);
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		doFinished(true);
	}

	public int getId() {
		return id;
	}

	public boolean isFinished() {
		return finished;
	}

	private void doFinished(boolean failed) {
		this.finished = true;
		this.jobHandler.finished(this.dataNodeOrDatabase, failed);
		if (this.ctx != null) {
			this.ctx.onJobFinished(this);
		}
	}

	@Override
	public String toString() {
		return "SQLJob [ id=" + id + ",dataNodeOrDatabase="
				+ dataNodeOrDatabase + ",sql=" + sql + ",  jobHandler="
				+ jobHandler + "]";
	}

}
