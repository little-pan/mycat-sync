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
package org.opencloudb.heartbeat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDataSource;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.mysql.nio.MySQLDataSource;
import org.opencloudb.sqlengine.OneRawSQLQueryResultHandler;
import org.opencloudb.sqlengine.SQLJob;
import org.opencloudb.sqlengine.SQLQueryResult;
import org.opencloudb.sqlengine.SQLQueryResultListener;
import org.opencloudb.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public class MySQLDetector implements
		SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {

	static final Logger log = LoggerFactory.getLogger(MySQLDetector.class);

	private MySQLHeartbeat heartbeat;
	private long heartbeatTimeout;
	private final AtomicBoolean isQuit;
	private volatile long lastSendQryTime;
	private volatile long lasstReveivedQryTime;
	private volatile SQLJob sqlJob;

	private static final String[] MYSQL_SLAVE_STAUTS_COLMS = new String[] {
			"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running","Slave_IO_State","Master_Host","Master_User","Master_Port", 
			"Connect_Retry","Last_IO_Error"};

	private static final String[] MYSQL_CLUSTER_STAUTS_COLMS = new String[] {"Variable_name","Value"};
	/*private static final String[] MYSQL_CLUSTER_STAUTS_COLMS = new String[] {
			"wsrep_incoming_addresses","wsrep_cluster_size","wsrep_cluster_status", "wsrep_connected", "wsrep_flow_control_paused",
			"wsrep_local_state_comment","wsrep_ready","wsrep_flow_control_paused_ns","wsrep_flow_control_recv","wsrep_local_bf_aborts", 
			"wsrep_local_recv_queue_avg","wsrep_local_send_queue_avg","wsrep_apply_oool","wsrep_apply_oooe"};*/
	
	public MySQLDetector(MySQLHeartbeat heartbeat) {
		this.heartbeat = heartbeat;
		this.isQuit = new AtomicBoolean(false);
	}

	public MySQLHeartbeat getHeartbeat() {
		return heartbeat;
	}

	public long getHeartbeatTimeout() {
		return heartbeatTimeout;
	}

	public void setHeartbeatTimeout(long heartbeatTimeout) {
		this.heartbeatTimeout = heartbeatTimeout;
	}

	public boolean isHeartbeatTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastSendQryTime,
				lasstReveivedQryTime) + heartbeatTimeout;
	}

	public long getLastSendQryTime() {
		return lastSendQryTime;
	}

	public long getLasstReveivedQryTime() {
		return lasstReveivedQryTime;
	}

	public void heartbeat() {
		this.lastSendQryTime = System.currentTimeMillis();
		MySQLDataSource ds = this.heartbeat.getSource();
		String databaseName = ds.getDbPool().getSchemas()[0];
		String[] fetchColumns = {};
		if (this.heartbeat.getSource().getHostConfig().isShowSlaveSql() ) {
			fetchColumns = MYSQL_SLAVE_STAUTS_COLMS;
		}
		if (this.heartbeat.getSource().getHostConfig().isShowClusterSql() ) {
			fetchColumns = MYSQL_CLUSTER_STAUTS_COLMS;
		}
		OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(fetchColumns, this);
		this.sqlJob = new SQLJob(this.heartbeat.getHeartbeatSQL(), databaseName, resultHandler, ds);
		this.sqlJob.run();
	}

	public void quit() {
		if (this.isQuit.compareAndSet(false, true)) {
			close("heart beat quit");
		}
	}

	public boolean isQuit() {
		return isQuit.get();
	}

	@Override
	public void onResult(SQLQueryResult<Map<String, String>> result) {
		if (result.isSuccess()) {
            int balance = this.heartbeat.getSource().getDbPool().getBalance();
			PhysicalDataSource source = this.heartbeat.getSource();
            int switchType = source.getHostConfig().getSwitchType();
            Map<String, String> resultResult = result.getResult();
            
            if(PhysicalDBPool.BALANCE_NONE!=balance && switchType==DataHostConfig.SYN_STATUS_SWITCH_DS
            		&& source.getHostConfig().isShowSlaveSql())
            {
                String Slave_IO_Running =resultResult!=null? resultResult.get("Slave_IO_Running"):null;
				String Slave_SQL_Running = resultResult!=null?resultResult.get("Slave_SQL_Running"):null;
				if (Slave_IO_Running != null && Slave_IO_Running.equals(Slave_SQL_Running) && Slave_SQL_Running.equals("Yes")) {
					this.heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
					String Seconds_Behind_Master = resultResult.get( "Seconds_Behind_Master");
					if (null != Seconds_Behind_Master && !"".equals(Seconds_Behind_Master)) {
						this.heartbeat.setSlaveBehindMaster(Integer.valueOf(Seconds_Behind_Master));
					}
				} else  if(source.isSalveOrRead()){
					String Last_IO_Error = resultResult!=null?resultResult.get("Last_IO_Error"):null;
					log.warn("Found MySQL master/slave Replication error! DataSource config {}, last error {}",
							this.heartbeat.getSource().getConfig(), Last_IO_Error);
					this.heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
				}
				this.heartbeat.getAsynRecorder().set(resultResult, switchType);
				this.heartbeat.setResult(MySQLHeartbeat.OK_STATUS, this,  null);
            }
            else if(PhysicalDBPool.BALANCE_NONE!=balance && switchType==DataHostConfig.CLUSTER_STATUS_SWITCH_DS 
            		&& source.getHostConfig().isShowClusterSql()) {
            	String wsrep_cluster_status = resultResult!=null? resultResult.get("wsrep_cluster_status"):null;//Primary
				String wsrep_connected = resultResult!=null?resultResult.get("wsrep_connected"):null;//ON
				String wsrep_ready = resultResult!=null?resultResult.get("wsrep_ready"):null;//ON
				if("ON".equals(wsrep_connected) && "ON".equals(wsrep_ready) && "Primary".equals(wsrep_cluster_status)){
					this.heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
					this.heartbeat.setResult(MySQLHeartbeat.OK_STATUS, this,  null);
				}else{
					log.warn("found MySQL  cluster status err !!! " + heartbeat.getSource().getConfig()
							+ " wsrep_cluster_status: "+ wsrep_cluster_status  
							+ " wsrep_connected: "+ wsrep_connected
							+ " wsrep_ready: "+ wsrep_ready
					);
					this.heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
					this.heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS, this,  null);
				}
				this.heartbeat.getAsynRecorder().set(resultResult, switchType);
    		} else {
				this.heartbeat.setResult(MySQLHeartbeat.OK_STATUS, this,  null);
    		}
		} else {
			this.heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS, this,  null);
		}
		this.lasstReveivedQryTime = System.currentTimeMillis();
		this.heartbeat.getRecorder().set((this.lasstReveivedQryTime - this.lastSendQryTime));
	}

	public void close(String msg) {
		SQLJob curJob = this.sqlJob;
		if (curJob != null && !curJob.isFinished()) {
			curJob.terminate(msg);
			this.sqlJob = null;
		}
	}

}
