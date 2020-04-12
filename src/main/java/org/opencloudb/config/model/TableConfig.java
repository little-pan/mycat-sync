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
package org.opencloudb.config.model;

import java.util.*;

import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.util.SplitUtil;

/**
 * @author mycat
 */
public class TableConfig {

	public static final int TYPE_GLOBAL_TABLE = 1;
	public static final int TYPE_GLOBAL_DEFAULT = 0;

	private final String name;
	private final String primaryKey;
	private final boolean autoIncrement;
	private final boolean needAddLimit;
	private final Set<String> dbTypes;
	private final int tableType;
	private final ArrayList<String> dataNodes;
	private final RuleConfig rule;
	private final String partitionColumn;
	private final boolean ruleRequired;
	private final TableConfig parentTC;
	private final boolean childTable;
	private final String joinKey;
	private final String parentKey;
	private final String locateRTableKeySql;
	// Only has one level of parent
	private final boolean secondLevel;
	private final boolean partitionKeyIsPrimaryKey;
	private final boolean joinKeyIsPartitionColumn;
	private final Random rand = new Random();

	public TableConfig(String name, String primaryKey, boolean autoIncrement,boolean needAddLimit, int tableType,
			String dataNode,Set<String> dbType, RuleConfig rule, boolean ruleRequired,
			TableConfig parentTC, boolean isChildTable, String joinKey,
			String parentKey) throws IllegalArgumentException {

		if (name == null) {
			throw new IllegalArgumentException("table name is null");
		}
		if (dataNode == null) {
			throw new IllegalArgumentException("dataNode name is null");
		}
		String[] dataNodes = SplitUtil.split(dataNode, ',', '$', '-');
		if (dataNodes.length <= 0) {
			throw new IllegalArgumentException("Invalid table dataNodes: " + dataNode);
		}
		if (ruleRequired && rule == null) {
			throw new IllegalArgumentException("ruleRequired but rule is null");
		}

		this.primaryKey = primaryKey;
		this.autoIncrement = autoIncrement;
		this.needAddLimit = needAddLimit;
		this.tableType = tableType;
		this.dbTypes = dbType;
		this.name = name.toUpperCase();

		this.dataNodes = new ArrayList<>(dataNodes.length);
		this.dataNodes.addAll(Arrays.asList(dataNodes));
		this.ruleRequired = ruleRequired;
		this.rule = rule;
		this.partitionColumn = (rule == null) ? null : rule.getColumn();
		this.partitionKeyIsPrimaryKey = Objects.equals(this.partitionColumn, primaryKey);
		this.childTable = isChildTable;
		this.parentTC = parentTC;
		this.joinKey = joinKey;
		this.parentKey = parentKey;

		if (parentTC != null) {
			this.locateRTableKeySql = genLocateRootParentSQL();
			this.secondLevel = (parentTC.parentTC == null);
		} else {
			this.locateRTableKeySql = null;
			this.secondLevel = false;
		}
		this.joinKeyIsPartitionColumn = this.secondLevel
				&& parentTC.getPartitionColumn().equals(parentKey);
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

    public Set<String> getDbTypes()
    {
        return dbTypes;
    }

    public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public boolean isNeedAddLimit() {
		return needAddLimit;
	}

	public boolean isSecondLevel() {
		return secondLevel;
	}

	public String getLocateRTableKeySql() {
		return locateRTableKeySql;
	}

	public boolean isGlobalTable() {
		return this.tableType == TableConfig.TYPE_GLOBAL_TABLE;
	}

	public String getPartitionColumn() {
		return partitionColumn;
	}

	public int getTableType() {
		return tableType;
	}

	public TableConfig getParentTC() {
		return parentTC;
	}

	public boolean isChildTable() {
		return childTable;
	}

	public boolean isErTable() {
		return isChildTable();
	}

	public String getJoinKey() {
		return joinKey;
	}

	public String getParentKey() {
		return parentKey;
	}

	/**
	 * @return upper-case
	 */
	public String getName() {
		return name;
	}

	public ArrayList<String> getDataNodes() {
		return dataNodes;
	}

	public String getRandomDataNode() {
		int index = Math.abs(this.rand.nextInt()) % this.dataNodes.size();
		return this.dataNodes.get(index);
	}

	public boolean isRuleRequired() {
		return ruleRequired;
	}

	public RuleConfig getRule() {
		return rule;
	}

	public boolean primaryKeyIsPartitionKey() {
		return partitionKeyIsPrimaryKey;
	}

	public boolean joinKeyIsPartitionColumn() {
		return this.joinKeyIsPartitionColumn;
	}

	/**
	 * Get root parent config.
	 *
	 * @return The root parent of this table config
	 */
	public TableConfig getRootParent() {
		TableConfig parent = this.parentTC;
		TableConfig prev = parent;

		while (parent != null) {
			prev = parent;
			parent = prev.getParentTC();
		}

		return prev;
	}

	private String genLocateRootParentSQL() {
		TableConfig tc = this;
		TableConfig pc = tc.parentTC;
		if (pc == null) {
			throw new IllegalStateException("Table '" + this.name + "' not an ER table");
		}

		StringBuilder tables = new StringBuilder();
		StringBuilder conditions = new StringBuilder();
		TableConfig prev = null;
		String lastCond = "";
		int level = 0;
		while (pc != null) {
			tables.append(level > 0? ',': "").append(pc.name);

			if (level == 0) {
				lastCond = pc.name + '.' + tc.parentKey + '=';
			} else {
				conditions
						.append(pc.name).append('.').append(tc.parentKey)
						.append('=')
						.append(tc.name).append('.').append(tc.joinKey)
						.append(" AND ");
			}

			level++;
			prev = tc;
			tc = pc;
			pc = tc.parentTC;

			if (pc == null) {
				conditions.append(lastCond);
			}
		}

		return "SELECT " + tc.name + '.' + prev.parentKey
				+ " FROM "  + tables
				+ " WHERE " + conditions;
	}

}