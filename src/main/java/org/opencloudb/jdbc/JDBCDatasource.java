package org.opencloudb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.net.ConnectionManager;
import org.opencloudb.util.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCDatasource extends PhysicalDatasource {

    static final Logger log = LoggerFactory.getLogger(JDBCDatasource.class);

	static {
		List<String> drivers = Arrays.asList("com.mysql.jdbc.Driver", "org.postgresql.Driver",
                "oracle.jdbc.OracleDriver",                           "com.ibm.db2.jcc.DB2Driver",
                "org.hsqldb.jdbcDriver",                              "org.h2.Driver",
                "org.apache.derby.jdbc.ClientDriver",                 "org.sqlite.JDBC",
                "com.microsoft.sqlserver.jdbc.SQLServerDriver",       "org.opencloudb.jdbc.mongodb.MongoDriver",
                "org.opencloudb.jdbc.sequoiadb.SequoiaDriver",        "org.apache.hive.jdbc.HiveDriver");
		for (String driver : drivers) {
			try {
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
                log.debug("JDBC driver '{}' not in classpath", driver);
			}
		}
	}
	public JDBCDatasource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
		super(config, hostConfig, isReadNode);
	}

	@Override
	public DBHeartbeat createHeartBeat() {
		return new JDBCHeartbeat(this);
	}

	@Override
	public void createNewConnection(ResponseHandler handler, String schema) throws IOException {
		try {
			DBHostConfig cfg = getConfig();
			JDBCConnection c = new JDBCConnection();
			ConnectionManager manager = MycatServer.getInstance().getConnectionManager();
			manager.addBackend(c);

			c.setHost(cfg.getIp());
			c.setPort(cfg.getPort());
			c.setPool(this);
			c.setSchema(schema);
			c.setDbType(cfg.getDbType());
			c.setManager(manager);
			c.setId(PhysicalDatasource.ID_GENERATOR.incrementAndGet());

			Connection con = getConnection();
			c.setCon(con);
			// notify handler
			handler.connectionAcquired(c);
		} catch (Throwable cause) {
			handler.connectionError(cause, null);
		}
	}

    Connection getConnection() throws SQLException {
        DBHostConfig cfg = getConfig();
		Connection connection = DriverManager.getConnection(cfg.getUrl(), cfg.getUser(), cfg.getPassword());
		String initSql = getHostConfig().getConnectionInitSql();
		if(initSql != null && !"".equals(initSql)) {
		    Statement statement = null;
			try {
				 statement = connection.createStatement();
				 statement.execute(initSql);
			} finally {
				IoUtil.close(statement);
			}
		}

		return connection;
    }

}
