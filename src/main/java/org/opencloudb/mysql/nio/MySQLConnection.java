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
package org.opencloudb.mysql.nio;

import org.opencloudb.MycatServer;
import org.opencloudb.config.Capabilities;
import org.opencloudb.config.Isolations;
import org.opencloudb.exception.UnknownTxIsolationException;
import org.opencloudb.mysql.CharsetUtil;
import org.opencloudb.mysql.SecurityUtil;
import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.net.BackendException;
import org.opencloudb.net.mysql.*;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.TimeUtil;
import org.slf4j.*;

import java.io.UnsupportedEncodingException;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mycat
 */
public class MySQLConnection extends BackendConnection {

	private static final Logger log = LoggerFactory.getLogger(MySQLConnection.class);

	private static final long CLIENT_FLAGS = initClientFlags();

	private static long initClientFlags() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		MycatServer server = MycatServer.getContextServer();
		boolean usingCompress = server.getConfig().getSystem().getUseCompression()==1 ;
		if(usingCompress)
		{
			 flag |= Capabilities.CLIENT_COMPRESS;
		}
		flag |= Capabilities.CLIENT_ODBC;
		flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= Capabilities.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		// client extension
		flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
		flag |= Capabilities.CLIENT_MULTI_RESULTS;
		return flag;
	}

	private static final CommandPacket _READ_UNCOMMITTED = new CommandPacket();
	private static final CommandPacket _READ_COMMITTED = new CommandPacket();
	private static final CommandPacket _REPEATED_READ = new CommandPacket();
	private static final CommandPacket _SERIALIZABLE = new CommandPacket();
	private static final CommandPacket _AUTOCOMMIT_ON = new CommandPacket();
	private static final CommandPacket _AUTOCOMMIT_OFF = new CommandPacket();
	private static final CommandPacket _COMMIT = new CommandPacket();
	private static final CommandPacket _ROLLBACK = new CommandPacket();
	static {
		_READ_UNCOMMITTED.packetId = 0;
		_READ_UNCOMMITTED.command = MySQLPacket.COM_QUERY;
		_READ_UNCOMMITTED.arg = "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
				.getBytes();
		_READ_COMMITTED.packetId = 0;
		_READ_COMMITTED.command = MySQLPacket.COM_QUERY;
		_READ_COMMITTED.arg = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"
				.getBytes();
		_REPEATED_READ.packetId = 0;
		_REPEATED_READ.command = MySQLPacket.COM_QUERY;
		_REPEATED_READ.arg = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
				.getBytes();
		_SERIALIZABLE.packetId = 0;
		_SERIALIZABLE.command = MySQLPacket.COM_QUERY;
		_SERIALIZABLE.arg = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"
				.getBytes();
		_AUTOCOMMIT_ON.packetId = 0;
		_AUTOCOMMIT_ON.command = MySQLPacket.COM_QUERY;
		_AUTOCOMMIT_ON.arg = "SET autocommit=1".getBytes();
		_AUTOCOMMIT_OFF.packetId = 0;
		_AUTOCOMMIT_OFF.command = MySQLPacket.COM_QUERY;
		_AUTOCOMMIT_OFF.arg = "SET autocommit=0".getBytes();
		_COMMIT.packetId = 0;
		_COMMIT.command = MySQLPacket.COM_QUERY;
		_COMMIT.arg = "commit".getBytes();
		_ROLLBACK.packetId = 0;
		_ROLLBACK.command = MySQLPacket.COM_QUERY;
		_ROLLBACK.arg = "rollback".getBytes();
	}

	private String oldSchema;
	private int batchCmdCount = 0;

	private long threadId;
	private HandshakePacket handshake;
	private long clientFlags;
	private boolean isAuthenticated;
	private String user;
	private String password;

	private StatusSync statusSync;
	private boolean metaDataSyned = true;
	private int xaStatus = 0;

	public MySQLConnection(SocketChannel channel, boolean fromSlaveDB) {
		super(channel);
		this.clientFlags = CLIENT_FLAGS;
		this.fromSlaveDB = fromSlaveDB;
	}

	public int getXaStatus() {
		return xaStatus;
	}

	public void setXaStatus(int xaStatus) {
		this.xaStatus = xaStatus;
	}

	public void onConnectFailed(Throwable t) {
		if (this.handler instanceof MySQLConnectionHandler) {
			MySQLConnectionHandler ha = (MySQLConnectionHandler) handler;
			ha.connectionError(t);
		} else {
			((MySQLConnectionAuthenticator)this.handler).connectionError(this, t);
		}
	}

	@Override
	public void setSchema(String newSchema) {
		String curSchema = schema;
		if (curSchema == null) {
			this.schema = newSchema;
			this.oldSchema = newSchema;
		} else {
			this.oldSchema = curSchema;
			this.schema = newSchema;
		}
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public HandshakePacket getHandshake() {
		return handshake;
	}

	public void setHandshake(HandshakePacket handshake) {
		this.handshake = handshake;
	}

	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
	}

	public boolean isAuthenticated() {
		return isAuthenticated;
	}

	public void setAuthenticated(boolean isAuthenticated) {
		this.isAuthenticated = isAuthenticated;
	}

	public String getPassword() {
		return password;
	}

	public void authenticate() {
		AuthPacket packet = new AuthPacket();
		packet.packetId = 1;
		packet.clientFlags = clientFlags;
		packet.maxPacketSize = maxPacketSize;
		packet.charsetIndex = this.charsetIndex;
		packet.user = user;
		try {
			packet.password = passwd(password, handshake);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
		packet.database = schema;
		packet.write(this);
	}

	protected void sendQueryCmd(String query) {
		log.debug("send query '{}' in {}", query, this);

		CommandPacket packet = new CommandPacket();
		packet.packetId = 0;
		packet.command = MySQLPacket.COM_QUERY;
		try {
			packet.arg = query.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			throw new BackendException("charset '"+charset+"' not supported", e);
		}
		lastTime = TimeUtil.currentTimeMillis();
		packet.write(this);
	}

	private static void getCharsetCommand(StringBuilder sb, int clientCharIndex) {
		sb.append("SET names ").append(CharsetUtil.getCharset(clientCharIndex))
				.append(";");
	}

	private static void getTxIsolationCommand(StringBuilder sb, int txIsolation) {
		switch (txIsolation) {
		case Isolations.READ_UNCOMMITTED:
			sb.append("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
			return;
		case Isolations.READ_COMMITTED:
			sb.append("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;");
			return;
		case Isolations.REPEATED_READ:
			sb.append("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
			return;
		case Isolations.SERIALIZABLE:
			sb.append("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
			return;
		default:
			throw new UnknownTxIsolationException("txIsolation:" + txIsolation);
		}
	}

	private void getAutocommitCommand(StringBuilder sb, boolean autoCommit) {
		if (autoCommit) {
			sb.append("SET autocommit=1;");
		} else {
			sb.append("SET autocommit=0;");
		}
	}

	private static class StatusSync {
		private final String schema;
		private final Integer charsetIndex;
		private final Integer txtIsolation;
		private final Boolean autocommit;
		private final AtomicInteger synCmdCount;
		private final boolean xaStarted;

		public StatusSync(boolean xaStarted, String schema,
				Integer charsetIndex, Integer txtIsolation, Boolean autocommit,
				int synCount) {
			super();
			this.xaStarted = xaStarted;
			this.schema = schema;
			this.charsetIndex = charsetIndex;
			this.txtIsolation = txtIsolation;
			this.autocommit = autocommit;
			this.synCmdCount = new AtomicInteger(synCount);
		}

		public boolean synAndExecuted(MySQLConnection conn) {
			int remains = synCmdCount.decrementAndGet();
			if (remains == 0) {// syn command finished
				this.updateConnectionInfo(conn);
				conn.metaDataSyned = true;
				return false;
			} else if (remains < 0) {
				return true;
			}
			return false;
		}

		private void updateConnectionInfo(MySQLConnection conn) {
			conn.xaStatus = xaStarted? 1 : 0;
			if (schema != null) {
				conn.schema = schema;
				conn.oldSchema = conn.schema;
			}
			if (charsetIndex != null) {
				conn.setCharset(CharsetUtil.getCharset(charsetIndex));
			}
			if (txtIsolation != null) {
				conn.txIsolation = txtIsolation;
			}
			if (autocommit != null) {
				conn.autocommit = autocommit;
			}
		}
	}

	/**
	 * @return if synchronization finished and execute-sql has already been sent before
	 */
	@Override
	public boolean syncAndExecute() {
		StatusSync sync = this.statusSync;
		if (sync == null) {
			return true;
		} else {
			boolean executed = sync.synAndExecuted(this);
			if (executed) {
				statusSync = null;
			}
			return executed;
		}
	}

	public void execute(RouteResultsetNode rrn, ServerConnection sc, boolean autocommit) {
		if (!modifiedSQLExecuted && rrn.isModifySQL()) {
			modifiedSQLExecuted = true;
		}
		String xaTXID = sc.getSession().getXaTXID();
		synAndDoExecute(xaTXID, rrn, sc.getCharsetIndex(), sc.getTxIsolation(), autocommit);
	}

	private void synAndDoExecute(String xaTxID, RouteResultsetNode rrn,
			int clientCharSetIndex, int clientTxIsolation, boolean clientAutoCommit) {
		String xaCmd = null;

		boolean conAutoCommit = this.autocommit;
		String conSchema = this.schema;
		// never executed modify sql,so auto commit
		boolean expectAutocommit = !modifiedSQLExecuted || isFromSlaveDB()
				|| clientAutoCommit;
		if (!expectAutocommit && xaTxID != null && xaStatus == 0) {
			clientTxIsolation = Isolations.SERIALIZABLE;
			xaCmd = "XA START " + xaTxID + ';';
		}
		int schemaSyn = conSchema.equals(oldSchema) ? 0 : 1;
		int charsetSyn = 0;
		if (this.charsetIndex != clientCharSetIndex) {
			//need to syn the charset of connection.
			//set current connection charset to client charset.
			//otherwise while sending commend to server the charset will not coincidence.
			setCharset(CharsetUtil.getCharset(clientCharSetIndex));
			charsetSyn = 1;
		}
		int txIsolationSync = (txIsolation == clientTxIsolation) ? 0 : 1;
		int autoCommitSyn = (conAutoCommit == expectAutocommit) ? 0 : 1;
		int synCount = schemaSyn + charsetSyn + txIsolationSync + autoCommitSyn;
		if (synCount == 0) {
			// not need syn connection
			sendQueryCmd(rrn.getStatement());
			return;
		}
		CommandPacket schemaCmd = null;
		StringBuilder sb = new StringBuilder();
		if (schemaSyn == 1) {
			schemaCmd = getChangeSchemaCommand(conSchema);
		}

		if (charsetSyn == 1) {
			getCharsetCommand(sb, clientCharSetIndex);
		}
		if (txIsolationSync == 1) {
			getTxIsolationCommand(sb, clientTxIsolation);
		}
		if (autoCommitSyn == 1) {
			getAutocommitCommand(sb, expectAutocommit);
		}
		if (xaCmd != null) {
			sb.append(xaCmd);
		}
		if (log.isDebugEnabled()) {
			log.debug("con need sync: total sync cmd {}, commands '{}', schema change {} in backend {}" ,
					synCount, sb, schemaCmd != null, this);
		}
		metaDataSyned = false;
		statusSync = new StatusSync(xaCmd != null, conSchema,
				clientCharSetIndex, clientTxIsolation, expectAutocommit,
				synCount);
		// syn schema
		if (schemaCmd != null) {
			schemaCmd.write(this);
		}
		// and our query sql to multi command at last
		sb.append(rrn.getStatement());
		// syn and execute others
		sendQueryCmd(sb.toString());
		// waiting syn result...
	}

	private static CommandPacket getChangeSchemaCommand(String schema) {
		CommandPacket cmd = new CommandPacket();
		cmd.packetId = 0;
		cmd.command = MySQLPacket.COM_INIT_DB;
		cmd.arg = schema.getBytes();
		return cmd;
	}

	/**
	 * by wuzh ,execute a query and ignore transaction settings for performance
	 * 
	 * @param query
	 */
	public void query(String query) {
		RouteResultsetNode rrn = new RouteResultsetNode("default", ServerParse.SELECT, query);
		synAndDoExecute(null, rrn, this.charsetIndex, this.txIsolation, true);
	}

	public void quit() {
		if (isClosedOrQuit()) {
			return;
		}

		if (this.isAuthenticated) {
			write(writeToBuffer(QuitPacket.QUIT, allocate()));
			write(allocate());
		} else {
			close("normal");
		}
	}

	@Override
	public void commit() {
		_COMMIT.write(this);
	}

	public boolean batchCmdFinished() {
		batchCmdCount--;
		return (batchCmdCount == 0);
	}

	public void execCmd(String cmd) {
		this.sendQueryCmd(cmd);
	}

	public void execBatchCmd(String[] batchCmds) {
		// "XA END "+xaID+";"+"XA PREPARE "+xaID
		this.batchCmdCount = batchCmds.length;
		StringBuilder sb = new StringBuilder();
		for (String sql : batchCmds) {
			sb.append(sql).append(';');
		}
		this.sendQueryCmd(sb.toString());
	}

	@Override
	public void rollback() {
		_ROLLBACK.write(this);
	}

	@Override
	public void release() {
		super.release();

		if (!this.metaDataSyned) {
			// indicate connection not normally finished, and
			// we can't know it's sync status, so close it
			log.warn("Can't sure connection sync result, so close {} ", this);
			this.respHandler = null;
			this.close("Sync status unknown");
			return;
		}

		this.metaDataSyned = true;
		this.attachment = null;
		this.statusSync = null;
		this.modifiedSQLExecuted = false;
		setResponseHandler(null);
		getPool().releaseChannel(this);
	}

	@Override
	public boolean setResponseHandler(ResponseHandler queryHandler) {
		if (this.handler instanceof MySQLConnectionHandler) {
			MySQLConnectionHandler ch = (MySQLConnectionHandler)this.handler;
			ch.setResponseHandler(queryHandler);
			this.respHandler = queryHandler;
			return true;
		} else if (queryHandler != null) {
			log.warn("set not MySQLConnectionHandler '{}'",
					queryHandler.getClass().getCanonicalName());
		}
		return false;
	}

	@Override
	public ResponseHandler getResponseHandler() {
		return this.respHandler;
	}

	private static byte[] passwd(String pass, HandshakePacket hs)
			throws NoSuchAlgorithmException {
		if (pass == null || pass.length() == 0) {
			return null;
		}
		byte[] passwd = pass.getBytes();
		int sl1 = hs.seed.length;
		int sl2 = hs.restOfScrambleBuff.length;
		byte[] seed = new byte[sl1 + sl2];
		System.arraycopy(hs.seed, 0, seed, 0, sl1);
		System.arraycopy(hs.restOfScrambleBuff, 0, seed, sl1, sl2);
		return SecurityUtil.scramble411(passwd, seed);
	}

	@Override
	public String toString() {
		return "MySQLConnection [id=" + id + ", lastTime=" + lastTime
				+ ", user=" + user
				+ ", schema=" + schema + ", old schema=" + oldSchema
				+ ", borrowed=" + borrowed + ", fromSlaveDB=" + fromSlaveDB
				+ ", threadId=" + threadId + ", charset=" + charset
				+ ", txIsolation=" + txIsolation + ", autocommit=" + autocommit
				+ ", attachment=" + attachment + ", respHandler=" + respHandler
				+ ", host=" + host + ", port=" + port + ", statusSync="
				+ statusSync + ", writeQueue=" + this.getWriteQueue().size()
				+ ", modifiedSQLExecuted=" + modifiedSQLExecuted + "]";
	}

}
