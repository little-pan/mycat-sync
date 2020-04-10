package org.opencloudb.parser.druid;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.opencloudb.mpp.ColumnRoutePair;
import org.opencloudb.mpp.RangeValue;

/**
 * 路由计算单元
 * 
 * @author wang.dw
 * @date 2015-3-14 下午6:24:54
 * @version 0.1.0 
 * @copyright wonhigh.cn
 */
public class RouteCalculateUnit {

	private Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = new LinkedHashMap<>();

	public Map<String, Map<String, Set<ColumnRoutePair>>> getTablesAndConditions() {
		return tablesAndConditions;
	}

	public void addShardingExpr(String tableName, String columnName, Object value) {
		Map<String, Set<ColumnRoutePair>> tableColumnsMap;
		
		if (value == null) {
			// where a=null
			return;
		}
		tableColumnsMap = this.tablesAndConditions.get(tableName);
		
		if (tableColumnsMap == null) {
			tableColumnsMap = new LinkedHashMap<>();
			this.tablesAndConditions.put(tableName, tableColumnsMap);
		}
		
		String uperColName = columnName.toUpperCase();
		Set<ColumnRoutePair> columValues = tableColumnsMap.get(uperColName);

		if (columValues == null) {
			columValues = new LinkedHashSet<>();
			tableColumnsMap.put(uperColName, columValues);
		}

		if (value instanceof Object[]) {
			for (Object item : (Object[]) value) {
				if(item == null) {
					continue;
				}
				columValues.add(new ColumnRoutePair(item.toString()));
			}
		} else if (value instanceof RangeValue) {
			columValues.add(new ColumnRoutePair((RangeValue) value));
		} else {
			columValues.add(new ColumnRoutePair(value.toString()));
		}
	}
	
	public void clear() {
		tablesAndConditions.clear();
	}
	
	
}
