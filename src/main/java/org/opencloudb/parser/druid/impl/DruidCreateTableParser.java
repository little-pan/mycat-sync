package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.parser.druid.MycatSchemaStatVisitor;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DruidCreateTableParser extends DefaultDruidParser {

	static final Logger log = LoggerFactory.getLogger(DruidCreateTableParser.class);

	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) {
	}
	
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement)stmt;
		if(createStmt.getQuery() != null) {
			String msg = "create table from other table not supported :" + stmt;
			log.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		String tableName = StringUtil.removeBackquote(createStmt.getTableSource().toString().toUpperCase());
		ctx.addTable(tableName);
	}

}
