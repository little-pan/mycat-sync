package org.opencloudb.route;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.server.ServerSession;

public class SessionSQLPair {
	public final ServerSession session;
	
	public final SchemaConfig schema;
	public final String sql;
	public final int type;

	public SessionSQLPair(ServerSession session, SchemaConfig schema, String sql,int type) {
		this.session = session;
		this.schema = schema;
		this.sql = sql;
		this.type=type;
	}

}
