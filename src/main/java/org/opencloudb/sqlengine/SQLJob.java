package org.opencloudb.sqlengine;

import java.util.List;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;
import org.slf4j.*;

/**
 * asyn execute in EngineCtx or standalone (EngineCtx=null)
 * 
 * @author wuzhih
 * 
 */
public class SQLJob implements ResponseHandler, Runnable {

	public static final Logger log = LoggerFactory.getLogger(SQLJob.class);

	private final String sql;
	private final String dataNodeOrDatabase;
	private BackendConnection connection;
	private final SQLJobHandler jobHandler;
	private final EngineCtx ctx;
	private final PhysicalDatasource ds;
	private final int id;
	private volatile boolean finished;

	public SQLJob(int id, String sql, String dataNode,
			SQLJobHandler jobHandler, EngineCtx ctx) {
		super();
		this.id = id;
		this.sql = sql;
		this.dataNodeOrDatabase = dataNode;
		this.jobHandler = jobHandler;
		this.ctx = ctx;
		this.ds = null;
	}

	public SQLJob(String sql, String databaseName, SQLJobHandler jobHandler,
			PhysicalDatasource ds) {
		super();
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
				RouteResultsetNode node = new RouteResultsetNode(dataNodeOrDatabase, ServerParse.SELECT, sql);
				// create new connection
				MycatConfig conf = MycatServer.getInstance().getConfig();
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				dn.getConnection(dn.getDatabase(), true, node, this, node);
			} else {
				this.ds.getConnection(dataNodeOrDatabase, true, null, this, null);
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
		log.debug("con query sql: '{}' to con: {}", this.sql, conn);

		conn.setResponseHandler(this);
		try {
			conn.query(sql);
			connection = conn;
		} catch (Exception e) {
			doFinished(true);
		}
	}

	public boolean isFinished() {
		return finished;
	}

	private void doFinished(boolean failed) {
		finished = true;
		jobHandler.finished(dataNodeOrDatabase, failed);
		if (ctx != null) {
			ctx.onJobFinished(this);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		log.warn("Can't get connection for sql: '" + sql + "'", e);
		doFinished(true);
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
		conn.syncAndExcute();
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		jobHandler.onHeader(dataNodeOrDatabase, header, fields);
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		boolean finished = jobHandler.onRowData(dataNodeOrDatabase, row);
		if (finished) {
			conn.close("not needed by user proc");
			doFinished(false);
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		conn.release();
		doFinished(false);
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		doFinished(true);
	}

	public int getId() {
		return id;
	}

	@Override
	public String toString() {
		return "SQLJob [ id=" + id + ",dataNodeOrDatabase="
				+ dataNodeOrDatabase + ",sql=" + sql + ",  jobHandler="
				+ jobHandler + "]";
	}

}
