package org.opencloudb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDataSource;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.net.ConnectionManager;
import org.opencloudb.util.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCDataSource extends PhysicalDataSource {

    static final Logger log = LoggerFactory.getLogger(JDBCDataSource.class);

	static {
		List<String> drivers = Arrays.asList("com.mysql.jdbc.Driver", "org.postgresql.Driver",
                "oracle.jdbc.OracleDriver",                           "com.ibm.db2.jcc.DB2Driver",
                "org.hsqldb.jdbcDriver",                              "org.h2.Driver",
                "org.apache.derby.jdbc.ClientDriver",                 "org.sqlite.JDBC",
                "com.microsoft.sqlserver.jdbc.SQLServerDriver",       "org.opencloudb.jdbc.mongodb.MongoDriver",
                "org.opencloudb.jdbc.sequoiadb.SequoiaDriver",        "org.apache.hive.jdbc.HiveDriver");
		for (String driver : drivers) {
			try {
				if ("org.h2.Driver".equals(driver)) {
					System.setProperty("h2.socketConnectTimeout", CONNECT_TIMEOUT + "");
				}
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
                log.debug("JDBC driver '{}' not in classpath", driver);
			}
		}
	}

	public JDBCDataSource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
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
			c.setId(PhysicalDataSource.ID_GENERATOR.incrementAndGet());

			Connection con = getConnection();
			c.setCon(con);
			// notify handler
			handler.connectionAcquired(c);
		} catch (Throwable cause) {
			handler.connectionError(cause, null);
		}
	}

    Connection getConnection() throws SQLException {
		Connection con;
		String initSql = getHostConfig().getConnectionInitSql();
        DBHostConfig cfg = getConfig();

		Properties props = new Properties();
		props.put("user", cfg.getUser());
		props.put("password", cfg.getPassword());
		addConnProps(props, cfg.getDbType());

		con = DriverManager.getConnection(cfg.getUrl(), props);
		boolean failed = true;
		try {
			if (initSql != null && !"".equals(initSql)) {
				log.info("Execute init sql '{}'", initSql);
				try (Statement s = con.createStatement()) {
					s.execute(initSql);
				}
			}
			failed = false;
			return con;
		} finally {
			if (failed) {
				IoUtil.close(con);
			}
		}
    }

    private void addConnProps (Properties props, String dbType) {
		int seconds = CONNECT_TIMEOUT / 1000;
		String prop;

		switch (dbType.toLowerCase()) {
			case "mysql":
			case "mariadb":
				props.put(prop = "connectTimeout", CONNECT_TIMEOUT);
				break;
			case "postgresql":
			case "pg":
				props.put(prop = "connectTimeout", seconds);
				break;
			case "oracle":
				props.put(prop = "oracle.net.CONNECT_TIMEOUT", CONNECT_TIMEOUT);
				break;
			case "hsqldb":
			case "hsql":
				props.put(prop = "loginTimeout", seconds);
				break;
			default:
				return;
		}
		log.debug("add property {}={}ms for {} connection", prop, CONNECT_TIMEOUT, dbType);
	}

}
