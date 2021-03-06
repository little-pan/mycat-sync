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
package org.opencloudb.config.loader.xml;

import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.model.*;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.config.util.ConfigUtil;
import org.opencloudb.util.DecryptUtil;
import org.opencloudb.util.IoUtil;
import org.opencloudb.util.SplitUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author mycat
 */
public class XMLSchemaLoader implements SchemaLoader {
	
	private final static String DEFAULT_DTD = "schema.dtd";
	private final static String DEFAULT_XML = "schema.xml";

	private final Map<String, TableRuleConfig> tableRules;
	private final Map<String, DataHostConfig> dataHosts;
	private final Map<String, DataNodeConfig> dataNodes;
	private final Map<String, SchemaConfig> schemas;

	public XMLSchemaLoader(String schemaFile, String ruleFile) {
		XMLRuleLoader ruleLoader = new XMLRuleLoader(ruleFile);
		this.tableRules = ruleLoader.getTableRules();
		this.dataHosts = new HashMap<>();
		this.dataNodes = new HashMap<>();
		this.schemas = new HashMap<>();
		this.load(DEFAULT_DTD, schemaFile == null ? DEFAULT_XML : schemaFile);
	}

	public XMLSchemaLoader() {
		this(null, null);
	}

	@Override
	public Map<String, TableRuleConfig> getTableRules() {
		return tableRules;
	}

	@Override
	public Map<String, DataHostConfig> getDataHosts() {
		return this.dataHosts;
	}

	@Override
	public Map<String, DataNodeConfig> getDataNodes() {
		return this.dataNodes;
	}

	@Override
	public Map<String, SchemaConfig> getSchemas() {
		return this.schemas;
	}

	private void load(String dtdFile, String xmlFile) {
		InputStream dtd = null;
		InputStream xml = null;
		try {
			dtd = SystemConfig.getConfigFileStream(dtdFile);
			xml = SystemConfig.getConfigFileStream(xmlFile);
			Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
			loadDataHosts(root);
			loadDataNodes(root);
			loadSchemas(root);
		} catch (ConfigException e) {
			throw e;
		} catch (Exception e) {
			throw new ConfigException(e);
		} finally {
			IoUtil.close(dtd);
			IoUtil.close(xml);
		}
	}

	private void loadSchemas(Element root) {
		NodeList list = root.getElementsByTagName("schema");
		for (int i = 0, n = list.getLength(); i < n; i++) {
			Element schemaElement = (Element) list.item(i);
			String name = schemaElement.getAttribute("name");
			String dataNode = schemaElement.getAttribute("dataNode");
			String checkSQLSchemaStr = schemaElement.getAttribute("checkSQLschema");
			String sqlMaxLimitStr = schemaElement.getAttribute("sqlMaxLimit");
			int sqlMaxLimit = -1;
			if (sqlMaxLimitStr != null && !sqlMaxLimitStr.isEmpty()) {
				sqlMaxLimit = Integer.parseInt(sqlMaxLimitStr);
			}
			
			// check dataNode already exists or not
			String defaultDbType = null;
			if (dataNode != null && !dataNode.isEmpty()) {
				List<String> dataNodeLst = new ArrayList<>(1);
				dataNodeLst.add(dataNode);
				checkDataNodeExists(dataNodeLst);
				String dataHost = dataNodes.get(dataNode).getDataHost();
				defaultDbType = dataHosts.get(dataHost).getDbType();
			} else {
				dataNode = null;
			}
			
			Map<String, TableConfig> tables = loadTables(schemaElement);
			if (schemas.containsKey(name)) {
				throw new ConfigException("schema " + name + " duplicated!");
			}

			// 设置了table的不需要设置dataNode属性，没有设置table的必须设置dataNode属性
			if (dataNode == null && tables.size() == 0) {
				throw new ConfigException(
						"schema " + name + " didn't config tables,so you must set dataNode property!");
			}

			SchemaConfig schemaConfig = new SchemaConfig(name, dataNode,
					tables, sqlMaxLimit, "true".equalsIgnoreCase(checkSQLSchemaStr));
			
			if (defaultDbType != null) {
				schemaConfig.setDefaultDataNodeDbType(defaultDbType);
				if (!"mysql".equalsIgnoreCase(defaultDbType)) {
					schemaConfig.setNeedSupportMultiDBType(true);
				}
			}

			// 判断是否有不是mysql的数据库类型，方便解析判断是否启用多数据库分页语法解析
			for (String tableName : tables.keySet()) {
				TableConfig tableConfig = tables.get(tableName);
				if (isHasMultiDbType(tableConfig)) {
					schemaConfig.setNeedSupportMultiDBType(true);
					break;
				}
			}
			Map<String, String> dataNodeDbTypeMap = new HashMap<>();
			for (String dataNodeName : dataNodes.keySet()) {
				DataNodeConfig dataNodeConfig = dataNodes.get(dataNodeName);
				String dataHost = dataNodeConfig.getDataHost();
				DataHostConfig dataHostConfig = dataHosts.get(dataHost);
				if (dataHostConfig != null) {
					String dbType = dataHostConfig.getDbType();
					dataNodeDbTypeMap.put(dataNodeName, dbType);
				}
			}
			schemaConfig.setDataNodeDbTypeMap(dataNodeDbTypeMap);
			schemas.put(name, schemaConfig);
		}
	}

	
	/**
	 * 处理动态日期表, 支持 YYYYMM、YYYYMMDD 两种格式
	 * 
	 * YYYYMM格式： 	  yyyymm,2015,01,60   
	 * YYYYMMDD格式:  yyyymmdd,2015,01,10,50
	 * 
	 * @param tableNameElement
	 * @param tableNameSuffixElement
	 * @return
	 */
	private String doTableNameSuffix(String tableNameElement, String tableNameSuffixElement) {
		String newTableName = tableNameElement;
		String[] params = tableNameSuffixElement.split(",");			
		String suffixFormat = params[0].toUpperCase();		
		if ( suffixFormat.equals("YYYYMM") ) {
			//读取参数
			int yyyy = Integer.parseInt( params[1] );
			int mm = Integer.parseInt( params[2] );					
			int mmEndIdx =  Integer.parseInt( params[3] );
			
			//日期处理
			SimpleDateFormat yyyyMMSDF = new SimpleDateFormat("yyyyMM"); 
			
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.YEAR, yyyy );
			cal.set(Calendar.MONTH, mm - 1 );
			cal.set(Calendar.DATE, 0 );
			
			//表名改写
			StringBuilder tableNameBuffer = new StringBuilder();
			for(int mmIdx = 0; mmIdx <= mmEndIdx; mmIdx++) {						
				tableNameBuffer.append( tableNameElement );
				tableNameBuffer.append( yyyyMMSDF.format(cal.getTime()) );							
				cal.add(Calendar.MONTH, 1);
				
				if ( mmIdx != mmEndIdx) {
					tableNameBuffer.append(",");
				}						
			}					
			newTableName = tableNameBuffer.toString();

		} else if ( suffixFormat.equals("YYYYMMDD") ) {
			//读取参数
			int yyyy = Integer.parseInt( params[1] );
			int mm = Integer.parseInt( params[2] );
			int dd =  Integer.parseInt( params[3] );
			int ddEndIdx =  Integer.parseInt( params[4] );
			
			//日期处理
			SimpleDateFormat yyyyMMddSDF = new SimpleDateFormat("yyyyMMdd"); 
			
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.YEAR, yyyy );
			cal.set(Calendar.MONTH, mm - 1 );
			cal.set(Calendar.DATE, dd );
			
			//表名改写
			StringBuilder tableNameBuffer = new StringBuilder();
			for(int ddIdx = 0; ddIdx <= ddEndIdx; ddIdx++) {					
				tableNameBuffer.append( tableNameElement );
				tableNameBuffer.append( yyyyMMddSDF.format(cal.getTime()) );
				
				cal.add(Calendar.DATE, 1);	
				
				if ( ddIdx != ddEndIdx) {
					tableNameBuffer.append(",");
				}
			}					
			newTableName = tableNameBuffer.toString();
		}				
		return newTableName;		
	}

	private Map<String, TableConfig> loadTables(Element node) {
		// 支持表名中包含引号[`] BEN GONG
		Map<String, TableConfig> tables = new TableConfigMap();
		
		NodeList nodeList = node.getElementsByTagName("table");
		for (int i = 0; i < nodeList.getLength(); i++) {
			Element tableElement = (Element) nodeList.item(i);
			String tableNameElement = tableElement.getAttribute("name").toUpperCase();

			// TODO:路由, 增加对动态日期表的支持
			String tableNameSuffixElement = tableElement.getAttribute("nameSuffix").toUpperCase();
			if ( !"".equals( tableNameSuffixElement ) ) {
				if( tableNameElement.split(",").length > 1 ) {
					throw new ConfigException("nameSuffix " + tableNameSuffixElement
							+ ", require name parameter cannot multiple breaks!");
				}
				
				tableNameElement = doTableNameSuffix(tableNameElement, tableNameSuffixElement);
			}
			
			String[] tableNames = tableNameElement.split(",");
			String primaryKey = tableElement.hasAttribute("primaryKey") ?
					tableElement.getAttribute("primaryKey").toUpperCase() : null;
			boolean autoIncrement = false;
			if (tableElement.hasAttribute("autoIncrement")) {
				autoIncrement = Boolean.parseBoolean(tableElement.getAttribute("autoIncrement"));
			}
			boolean needAddLimit = true;
			if (tableElement.hasAttribute("needAddLimit")) {
				needAddLimit = Boolean.parseBoolean(tableElement.getAttribute("needAddLimit"));
			}
			String tableTypeStr = tableElement.hasAttribute("type") ?
								tableElement.getAttribute("type") : null;
			int tableType = TableConfig.TYPE_GLOBAL_DEFAULT;
			if ("global".equalsIgnoreCase(tableTypeStr)) {
				tableType = TableConfig.TYPE_GLOBAL_TABLE;
			}
			String dataNode = tableElement.getAttribute("dataNode");
			TableRuleConfig tableRule = null;
			if (tableElement.hasAttribute("rule")) {
				String ruleName = tableElement.getAttribute("rule");
				tableRule = tableRules.get(ruleName);
				if (tableRule == null) {
					throw new ConfigException("rule " + ruleName + " is not found!");
				}
			}
			
			boolean ruleRequired = false;
			if (tableElement.hasAttribute("ruleRequired")) {
				ruleRequired = Boolean.parseBoolean(tableElement.getAttribute("ruleRequired"));
			}

			String distPrex = "distribute(";
			boolean distTableDns = dataNode.startsWith(distPrex);
			if (distTableDns) {
				dataNode = dataNode.substring(distPrex.length(), dataNode.length() - 1);
			}
			for (int j = 0; j < tableNames.length; j++) {
				String tableName = tableNames[j];
				TableConfig table = new TableConfig(tableName, primaryKey,
						autoIncrement, needAddLimit, tableType, dataNode,
						getDbType(dataNode),
						(tableRule != null) ? tableRule.getRule() : null,
						ruleRequired, null, false, null, null);
				
				checkDataNodeExists(table.getDataNodes());
				
				if (distTableDns) {
					distributeDataNodes(table.getDataNodes());
				}
				
				if (tables.containsKey(table.getName())) {
					throw new ConfigException("table " + tableName + " duplicated!");
				}
				
				tables.put(table.getName(), table);
			}

			if (tableNames.length == 1) {
				TableConfig table = tables.get(tableNames[0]);
				// Process child/er tables
				processChildTables(tables, table, dataNode, tableElement);
			}
		}

		return tables;
	}

	/**
	 * distribute datanodes in multi hosts,means ,dn1 (host1),dn100
	 * (host2),dn300(host3),dn2(host1),dn101(host2),dn301(host3)...etc
	 *
	 * @param dataNodes
	 */
	private void distributeDataNodes(ArrayList<String> dataNodes) {
		Map<String, ArrayList<String>> newDataNodeMap = new HashMap<>(dataHosts.size());
		for (String dn : dataNodes) {
			DataNodeConfig dnConf = this.dataNodes.get(dn);
			String host = dnConf.getDataHost();
			ArrayList<String> hostDns = newDataNodeMap.get(host);
			hostDns = (hostDns == null) ? new ArrayList<String>() : hostDns;
			hostDns.add(dn);
			newDataNodeMap.put(host, hostDns);
		}
		
		ArrayList<String> result = new ArrayList<>(dataNodes.size());
		boolean hasData = true;
		while (hasData) {
			hasData = false;
			for (ArrayList<String> dns : newDataNodeMap.values()) {
				if (!dns.isEmpty()) {
					result.add(dns.remove(0));
					hasData = true;
				}
			}
		}
		dataNodes.clear();
		dataNodes.addAll(result);
	}

	private Set<String> getDbType(String dataNode) {
		Set<String> dbTypes = new HashSet<>();
		String[] dataNodeArr = SplitUtil.split(dataNode, ',', '$', '-');
		for (String node : dataNodeArr) {
			DataNodeConfig datanode = dataNodes.get(node);
			DataHostConfig datahost = dataHosts.get(datanode.getDataHost());
			dbTypes.add(datahost.getDbType());
		}

		return dbTypes;
	}

	private Set<String> getDataNodeDbTypeMap(String dataNode) {
		Set<String> dbTypes = new HashSet<>();
		String[] dataNodeArr = SplitUtil.split(dataNode, ',', '$', '-');
		for (String node : dataNodeArr) {
			DataNodeConfig datanode = dataNodes.get(node);
			DataHostConfig datahost = dataHosts.get(datanode.getDataHost());
			dbTypes.add(datahost.getDbType());
		}
		return dbTypes;
	}

	private boolean isHasMultiDbType(TableConfig table) {
		Set<String> dbTypes = table.getDbTypes();
		for (String dbType : dbTypes) {
			if (!"mysql".equalsIgnoreCase(dbType)) {
				return true;
			}
		}
		return false;
	}

	private void processChildTables(Map<String, TableConfig> tables,
			TableConfig parentTable, String dataNodes, Element tableNode) {
		
		// parse child tables
		NodeList childNodeList = tableNode.getChildNodes();
		for (int j = 0; j < childNodeList.getLength(); j++) {
			Node theNode = childNodeList.item(j);
			if (!theNode.getNodeName().equals("childTable")) {
				continue;
			}
			Element childTbElement = (Element) theNode;

			String cdTbName = childTbElement.getAttribute("name").toUpperCase();
			String primaryKey = childTbElement.hasAttribute("primaryKey") ?
					childTbElement.getAttribute("primaryKey").toUpperCase() : null;

			boolean autoIncrement = false;
			if (childTbElement.hasAttribute("autoIncrement")) {
				autoIncrement = Boolean.parseBoolean(childTbElement.getAttribute("autoIncrement"));
			}
			boolean needAddLimit = true;
			if (childTbElement.hasAttribute("needAddLimit")) {
				needAddLimit = Boolean.parseBoolean(childTbElement.getAttribute("needAddLimit"));
			}
			String joinKey = childTbElement.getAttribute("joinKey").toUpperCase();
			String parentKey = childTbElement.getAttribute("parentKey").toUpperCase();
			TableConfig table = new TableConfig(cdTbName, primaryKey,
					autoIncrement, needAddLimit,
					TableConfig.TYPE_GLOBAL_DEFAULT, dataNodes,
					getDbType(dataNodes), null, false, parentTable, true,
					joinKey, parentKey);
			
			if (tables.containsKey(table.getName())) {
				throw new ConfigException("Table '" + table.getName() + "' duplicated!");
			}
			tables.put(table.getName(), table);
			processChildTables(tables, table, dataNodes, childTbElement);
		}
	}

	private void checkDataNodeExists(Collection<String> nodes) {
		if (nodes == null || nodes.size() < 1) {
			return;
		}
		for (String node : nodes) {
			if (!this.dataNodes.containsKey(node)) {
				throw new ConfigException("dataNode '" + node + "' is not found!");
			}
		}
	}

	private void loadDataNodes(Element root) {
		NodeList list = root.getElementsByTagName("dataNode");
		for (int i = 0, n = list.getLength(); i < n; i++) {
			Element element = (Element) list.item(i);
			String dnNamePre = element.getAttribute("name");

			String databaseStr = element.getAttribute("database");
			String host = element.getAttribute("dataHost");
			if (empty(dnNamePre) || empty(databaseStr) || empty(host)) {
				throw new ConfigException("dataNode " + dnNamePre + " defined error, attribute can't be empty");
			}
			String[] dnNames = org.opencloudb.util.SplitUtil.split(dnNamePre, ',', '$', '-');
			String[] databases = org.opencloudb.util.SplitUtil.split(databaseStr, ',', '$', '-');
			String[] hostStrings = org.opencloudb.util.SplitUtil.split(host, ',', '$', '-');

			if (dnNames.length > 1 && dnNames.length != databases.length * hostStrings.length) {
				throw new ConfigException("dataNode " + dnNamePre
								+ " define error ,dnNames.length must be=databases.length*hostStrings.length");
			}
			if (dnNames.length > 1) {
				List<String[]> mhdList = mergerHostDatabase(hostStrings, databases);
				for (int k = 0; k < dnNames.length; k++) {
					String[] hd = mhdList.get(k);
					String dnName = dnNames[k];
					String databaseName = hd[1];
					String hostName = hd[0];
					createDataNode(dnName, databaseName, hostName);
				}
			} else {
				createDataNode(dnNamePre, databaseStr, host);
			}
		}
	}

	private List<String[]> mergerHostDatabase(String[] hostStrings,	String[] databases) {
		List<String[]> mhdList = new ArrayList<>();
		for (int i = 0; i < hostStrings.length; i++) {
			String hostString = hostStrings[i];
			for (int i1 = 0; i1 < databases.length; i1++) {
				String database = databases[i1];
				String[] hd = new String[2];
				hd[0] = hostString;
				hd[1] = database;
				mhdList.add(hd);
			}
		}
		return mhdList;
	}

	private void createDataNode(String dnName, String database, String host) {
		
		DataNodeConfig conf = new DataNodeConfig(dnName, database, host);		
		if (dataNodes.containsKey(conf.getName())) {
			throw new ConfigException("dataNode " + conf.getName() + " duplicated!");
		}
		
		if (!dataHosts.containsKey(host)) {
			throw new ConfigException("dataNode " + dnName + " reference dataHost:" + host + " not exists!");
		}
		dataNodes.put(conf.getName(), conf);
	}

	private boolean empty(String dnName) {
		return dnName == null || dnName.length() == 0;
	}

	private DBHostConfig createDBHostConf(String dataHost, Element node,
			String dbType, String dbDriver, int maxCon, int minCon, String filters, long logTime) {

		String nodeHost = node.getAttribute("host");
		String nodeUrl = node.getAttribute("url");
		String user = node.getAttribute("user");
		String password = node.getAttribute("password");
		String usingDecrypt = node.getAttribute("usingDecrypt");
		String passwordEncrypt = DecryptUtil.DBHostDecrypt(usingDecrypt, nodeHost, user, password);
		
		String weightStr = node.getAttribute("weight");
		int weight = "".equals(weightStr) ? PhysicalDBPool.WEIGHT : Integer.parseInt(weightStr) ;
		
		String ip;
		int port;
		if (empty(nodeHost) || empty(nodeUrl) || empty(user)) {
			throw new ConfigException(
					"dataHost '" + dataHost
							+ "' defined error, some attributes of this element is empty: " + nodeHost);
		}
		if ("native".equalsIgnoreCase(dbDriver)) {
			int colonIndex = nodeUrl.indexOf(':');
			ip = nodeUrl.substring(0, colonIndex).trim();
			port = Integer.parseInt(nodeUrl.substring(colonIndex + 1).trim());
		} else {
			URI url;
			try {
				url = new URI(nodeUrl.substring(5));
			} catch (Exception e) {
				throw new ConfigException("Invalid jdbc url '" + nodeUrl + "' of " + dataHost);
			}
			ip = url.getHost();
			port = url.getPort();
		}

		DBHostConfig conf = new DBHostConfig(nodeHost, ip, port, nodeUrl, user, passwordEncrypt, password);
		conf.setDbType(dbType);
		conf.setMaxCon(maxCon);
		conf.setMinCon(minCon);
		conf.setFilters(filters);
		conf.setLogTime(logTime);
		conf.setWeight(weight); 	//新增权重

		return conf;
	}

	private void loadDataHosts(Element root) {
		NodeList list = root.getElementsByTagName("dataHost");
		for (int i = 0, n = list.getLength(); i < n; ++i) {
			final Element element = (Element) list.item(i);
			String name = element.getAttribute("name");
			
			if (this.dataHosts.containsKey(name)) {
				throw new ConfigException("dataHost name " + name + "duplicated!");
			}
			
			int maxCon = Integer.parseInt(element.getAttribute("maxCon"));
			int minCon = Integer.parseInt(element.getAttribute("minCon"));
			int balance = Integer.parseInt(element.getAttribute("balance"));
			
			String switchTypeStr = element.getAttribute("switchType");
			int switchType = switchTypeStr.equals("") ? -1 : Integer.parseInt(switchTypeStr);
			
			String slaveThresholdStr = element.getAttribute("slaveThreshold");
			int slaveThreshold = slaveThresholdStr.equals("") ? -1 : Integer.parseInt(slaveThresholdStr);
			
			//如果 tempReadHostAvailable 设置大于 0 则表示写主机如果挂掉， 临时的读服务依然可用
			String tempReadHostAvailableStr = element.getAttribute("tempReadHostAvailable");
			boolean tempReadHostAvailable = !tempReadHostAvailableStr.equals("")
												&& Integer.parseInt(tempReadHostAvailableStr) > 0;
			
			String writeTypStr = element.getAttribute("writeType");
			int writeType = "".equals(writeTypStr) ? PhysicalDBPool.WRITE_ONLYONE_NODE
														: Integer.parseInt(writeTypStr);

			String dbDriver = element.getAttribute("dbDriver");
			String dbType = element.getAttribute("dbType");
			String filters = element.getAttribute("filters");
			String logTimeStr = element.getAttribute("logTime");
			long logTime = "".equals(logTimeStr) ? PhysicalDBPool.LONG_TIME : Long.parseLong(logTimeStr) ;

			String heartbeatSQL = ""; // default: use jdbc4 validation
			NodeList heartbeats = element.getElementsByTagName("heartbeat");
			if (heartbeats.getLength() > 0) {
				heartbeatSQL = heartbeats.item(0).getTextContent();
			}

			NodeList connectionInitSqlList = element.getElementsByTagName("connectionInitSql");
			String initConSQL = null;
			if (connectionInitSqlList.getLength() > 0) {
				initConSQL = connectionInitSqlList.item(0).getTextContent();
			}
			
			NodeList writeNodes = element.getElementsByTagName("writeHost");
			DBHostConfig[] writeDbConfs = new DBHostConfig[writeNodes.getLength()];
			Map<Integer, DBHostConfig[]> readHostsMap = new HashMap<>(2);
			for (int w = 0; w < writeDbConfs.length; w++) {
				Element writeNode = (Element) writeNodes.item(w);
				writeDbConfs[w] = createDBHostConf(name, writeNode,
						dbType, dbDriver, maxCon, minCon,filters,logTime);
				NodeList readNodes = writeNode.getElementsByTagName("readHost");
				if (readNodes.getLength() != 0) {
					DBHostConfig[] readDbConfs = new DBHostConfig[readNodes.getLength()];
					for (int r = 0; r < readDbConfs.length; r++) {
						Element readNode = (Element) readNodes.item(r);
						readDbConfs[r] = createDBHostConf(name, readNode,
								dbType, dbDriver, maxCon, minCon,filters, logTime);
					}
					readHostsMap.put(w, readDbConfs);
				}
			}

			DataHostConfig hostConf = new DataHostConfig(name, dbType, dbDriver, 
					writeDbConfs, readHostsMap, switchType, slaveThreshold, tempReadHostAvailable);
			hostConf.setMaxCon(maxCon);
			hostConf.setMinCon(minCon);
			hostConf.setBalance(balance);
			hostConf.setWriteType(writeType);
			hostConf.setHeartbeatSQL(heartbeatSQL);
			hostConf.setConnectionInitSql(initConSQL);
			hostConf.setFilters(filters);
			hostConf.setLogTime(logTime);

			this.dataHosts.put(hostConf.getName(), hostConf);
		}
	}

}
