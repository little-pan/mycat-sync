package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.List;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DruidUpdateParser extends DefaultDruidParser {

	static final Logger log = LoggerFactory.getLogger(DruidUpdateParser.class);

	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		if(ctx.getTables() != null && ctx.getTables().size() > 1 && !schema.isNoSharding()) {
			String msg = "multi table related update not supported,tables:" + ctx.getTables();
			log.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		MySqlUpdateStatement update = (MySqlUpdateStatement)stmt;
		String tableName = StringUtil.removeBackquote(update.getTableName().getSimpleName().toUpperCase());
		
		List<SQLUpdateSetItem> updateSetItem = update.getItems();
		TableConfig tc = schema.getTables().get(tableName);

		if(RouterUtil.isNoSharding(schema,tableName)) {//整个schema都不分库或者该表不拆分
			RouterUtil.routeForTableMeta(rrs, schema, tableName, rrs.getStatement());
			rrs.setFinishedRoute(true);
			return;
		}

		String partitionColumn = tc.getPartitionColumn();
		String joinKey = tc.getJoinKey();
		if(tc.isGlobalTable() || (partitionColumn == null && joinKey == null)) {
			//修改全局表 update 受影响的行数
			RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), rrs.getStatement(),tc.isGlobalTable());
			rrs.setFinishedRoute(true);
			return;
		}
		
		if(updateSetItem != null && updateSetItem.size() > 0) {
			boolean hasParent = (schema.getTables().get(tableName).getParentTC() != null);
			for(SQLUpdateSetItem item : updateSetItem) {
				String column = StringUtil.removeBackquote(item.getColumn().toString().toUpperCase());
				if(partitionColumn != null && partitionColumn.equals(column)) {
					String msg = "partion key can't be updated " + tableName + "->" + partitionColumn;
					log.warn(msg);
					throw new SQLNonTransientException(msg);
				}
				if(hasParent) {
					if(column.equals(joinKey)) {
						String msg = "parent relation column can't be updated " + tableName + "->" + joinKey;
						log.warn(msg);
						throw new SQLNonTransientException(msg);
					}
					rrs.setCacheAble(true);
				}
			}
		}
		
		if(schema.getTables().get(tableName).isGlobalTable() && ctx.getRouteCalculateUnit().getTablesAndConditions().size() > 1) {
			throw new SQLNonTransientException("global table not supported multi table related update "+ tableName);
		}
	}
}
