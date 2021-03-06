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
package org.opencloudb.route;

import org.opencloudb.server.parser.ServerParse;

import java.io.Serializable;

/**
 * @author mycat
 */
public final class RouteResultsetNode implements Serializable , Comparable<RouteResultsetNode> {

	private static final long serialVersionUID = 1L;

	private final String name; // 数据节点名称
	private String statement; // 执行的语句
	private final String srcStatement;
	private final int sqlType;
	private volatile boolean canRunInReadDB;
	private final boolean hasBalanceFlag;

	private int limitStart;
	private int limitSize;
	private int totalNodeSize = 0; //方便后续jdbc批量获取扩展

	private String loadFile;

	public RouteResultsetNode(String name, int sqlType, String srcStatement) {
		this.name = name;
		this.limitStart = 0;
		this.limitSize = -1;
		this.sqlType = sqlType;
		this.srcStatement = srcStatement;
		this.statement = srcStatement;
		this.canRunInReadDB = (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW);
		this.hasBalanceFlag = (this.statement != null) && this.statement.startsWith("/*balance*/");
	}

	public void setStatement(String statement) {
		this.statement = statement;
	}

	public void setCanRunInReadDB(boolean canRunInReadDB) {
		this.canRunInReadDB = canRunInReadDB;
	}

	public void resetStatement() {
		this.statement = srcStatement;
	}

	public boolean canRunINReadDB(boolean autocommit) {
		if (!this.canRunInReadDB) {
			return false;
		}

		return (autocommit && !this.hasBalanceFlag
			|| !autocommit && this.hasBalanceFlag);
	}

	public String getName() {
		return name;
	}

	public int getSqlType() {
		return sqlType;
	}

	public String getStatement() {
		return statement;
	}

	public int getLimitStart()
	{
		return limitStart;
	}

	public void setLimitStart(int limitStart)
	{
		this.limitStart = limitStart;
	}

	public int getLimitSize()
	{
		return limitSize;
	}

	public void setLimitSize(int limitSize)
	{
		this.limitSize = limitSize;
	}

	public int getTotalNodeSize()
	{
		return totalNodeSize;
	}

	public void setTotalNodeSize(int totalNodeSize) {
		this.totalNodeSize = totalNodeSize;
	}

	public String getLoadFile()
	{
		return loadFile;
	}

	public void setLoadFile(String loadFile) {
		this.loadFile = loadFile;
	}

	@Override
	public int hashCode() {
		return this.name.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof RouteResultsetNode) {
			RouteResultsetNode rrn = (RouteResultsetNode) o;
			if (equals(this.name, rrn.getName())) {
				return true;
			}
		}

		return false;
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(this.name);
		s.append('{').append(this.statement).append('}');
		return s.toString();
	}

	private static boolean equals(String str1, String str2) {
		if (str1 == null) {
			return str2 == null;
		}
		return str1.equals(str2);
	}

	public boolean isModifySQL() {
		return !this.canRunInReadDB;
	}

	@Override
	public int compareTo(RouteResultsetNode obj) {
		if(obj == null) {
			return 1;
		}
		if(this.name == null) {
			return -1;
		}
		if(obj.name == null) {
			return 1;
		}
		return this.name.compareTo(obj.name);
	}

}
