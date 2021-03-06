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
package org.opencloudb.backend;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.config.Alarms;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.mysql.handler.ConnectionHeartBeatHandler;
import org.opencloudb.mysql.handler.DelegateResponseHandler;
import org.opencloudb.mysql.handler.NewConnectionRespHandler;
import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.net.NioProcessor;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.util.TimeUtil;
import org.slf4j.*;

/**
 * A physical represent of {read|write}Host.
 */
public abstract class PhysicalDataSource {

	private static final Logger log = LoggerFactory.getLogger(PhysicalDataSource.class);

	protected static final String INIT_WAIT_SECONDS_PROP = "org.opencloudb.backend.initWaitSeconds";
	protected static final int INIT_WAIT_SECONDS = Integer.getInteger(INIT_WAIT_SECONDS_PROP, 5);

	protected static final String CONNECT_TIMEOUT_PROP = "org.opencloudb.backend.connectTimeout";
	protected static final int CONNECT_TIMEOUT = Integer.getInteger(CONNECT_TIMEOUT_PROP, 3000);

	public static final AtomicLong ID_GENERATOR = new AtomicLong();

	private final String name;
	private final int size;
	private final DBHostConfig config;
	private final ConMap conMap = new ConMap();
	private final DBHeartbeat heartbeat;
	private final boolean readNode;
	private volatile long heartbeatRecoveryTime;
	private final DataHostConfig hostConfig;
	private final ConnectionHeartBeatHandler conHeartBeatHandler = new ConnectionHeartBeatHandler();
	private PhysicalDBPool dbPool;

	public PhysicalDataSource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
		this.size = config.getMaxCon();
		this.config = config;
		this.name = config.getHostName();
		this.hostConfig = hostConfig;
        this.heartbeat = this.createHeartBeat();
		this.readNode = isReadNode;
	}

	public DataHostConfig getHostConfig() {
		return hostConfig;
	}

	public boolean isReadNode() {
		return readNode;
	}

	public int getSize() {
		return size;
	}

	public void setDbPool(PhysicalDBPool dbPool) {
		this.dbPool = dbPool;
	}

	public PhysicalDBPool getDbPool() {
		return dbPool;
	}

	public abstract DBHeartbeat createHeartBeat();

	public String getName() {
		return name;
	}

	public int getInitWaitSeconds () {
		return PhysicalDataSource.INIT_WAIT_SECONDS;
	}

	public long getExecuteCount() {
		long executeCount = 0;
		for (ConQueue queue : conMap.getAllConQueue()) {
			executeCount += queue.getExecuteCount();

		}
		return executeCount;
	}

	public long getExecuteCountForSchema(String schema) {
		return conMap.getSchemaConQueue(schema).getExecuteCount();

	}

	public int getActiveCountForSchema(String schema) {
		return conMap.getActiveCountForSchema(schema, this);
	}

	public int getIdleCountForSchema(String schema) {
		ConQueue queue = conMap.getSchemaConQueue(schema);
		int total = 0;
		total += queue.getAutoCommitCons().size()
				+ queue.getManCommitCons().size();
		return total;
	}

	public DBHeartbeat getHeartbeat() {
		return heartbeat;
	}

	public int getIdleCount() {
		int total = 0;
		for (ConQueue queue : conMap.getAllConQueue()) {
			total += queue.getAutoCommitCons().size()
					+ queue.getManCommitCons().size();
		}
		return total;
	}

	private boolean validSchema(String schema) {
		return schema != null & !"".equals(schema) && !"snyn...".equals(schema);
	}

	private void checkIfNeedHeartBeat(List<BackendConnection> heartBeatCons, ConQueue queue,
									  Queue<BackendConnection> checkList, long hearBeatTime, long hearBeatTime2) {

		int maxConsInOneCheck = 10;
		Iterator<BackendConnection> it = checkList.iterator();
		while (it.hasNext()) {
			BackendConnection con = it.next();
			if (con.isClosedOrQuit()) {
				it.remove();
				continue;
			}
            if (validSchema(con.getSchema())) {
                if (con.getLastTime() < hearBeatTime && heartBeatCons.size() < maxConsInOneCheck) {
					it.remove();
                    // Heart beat check
                    con.setBorrowed(true);
                    heartBeatCons.add(con);
                }
            } else if (con.getLastTime() < hearBeatTime2) {
				// not valid schema connection should close for idle
				// exceed 2*conHeartBeatPeriod
				it.remove();
				con.close("heartbeat idle timeout");
			}
		}
	}

	public int getIndex() {
		int currentIndex = 0;
		for(int i = 0; i < dbPool.getSources().length; i++){
			PhysicalDataSource writeSource = dbPool.getSources()[i];
			if(writeSource.getName().equals(getName())){
				currentIndex = i;
				break;
			}
		}
		return currentIndex;
	}

	public boolean isSalveOrRead(){
		int currentIndex = getIndex();
		if(currentIndex != dbPool.activedIndex || this.readNode ){
			return true;
		}
		return false;
	}

	public void heatBeatCheck(long timeout, long conHeartBeatPeriod) {
		int maxConsInOneCheck = 5;
		final List<BackendConnection> heartBeatCons = new LinkedList<>();
		long hearBeatTime = TimeUtil.currentTimeMillis() - conHeartBeatPeriod;
		long hearBeatTime2 = TimeUtil.currentTimeMillis() - 2 * conHeartBeatPeriod;

		for (ConQueue queue : this.conMap.getAllConQueue()) {
			checkIfNeedHeartBeat(heartBeatCons, queue,
					queue.getAutoCommitCons(), hearBeatTime, hearBeatTime2);
			if (heartBeatCons.size() < maxConsInOneCheck) {
				checkIfNeedHeartBeat(heartBeatCons, queue,
						queue.getManCommitCons(), hearBeatTime, hearBeatTime2);
			} else {
				break;
			}
		}

		String sql = this.hostConfig.getHeartbeatSQL();
		for (BackendConnection con: heartBeatCons) {
			this.conHeartBeatHandler.heartBeat(con, sql);
		}

		// check if there has timeout heartbeat cons
		this.conHeartBeatHandler.abandonTimeoutConns();
		int idleCons = getIdleCount();
		int activeCons = this.getActiveCount();
		int createCount = (hostConfig.getMinCon() - idleCons) / 3;
		// create if idle too little
		if ((createCount > 0) && (idleCons + activeCons < size) && (idleCons < hostConfig.getMinCon())) {
			createByIdleLittle(idleCons, createCount);
        } else if (idleCons > hostConfig.getMinCon()) {
            closeByIdleMany(idleCons - hostConfig.getMinCon());
        } else {
			int activeCount = this.getActiveCount();
			if (activeCount > this.size) {
				StringBuilder s = new StringBuilder();
				s.append(Alarms.DEFAULT).append("dataSource exceed [name=")
						.append(name).append(",active=");
				s.append(activeCount).append(",size=").append(size).append(']');
				log.warn(s.toString());
			}
		}
	}

    private void closeByIdleMany(int idleCloseCount) {
        log.debug("Too many idle cons, close some for dataSource '{}'", this.name);
        List<BackendConnection> readyCloseCons = new ArrayList<>(idleCloseCount);
        for (ConQueue queue : this.conMap.getAllConQueue()) {
        	readyCloseCons.addAll(queue.getIdleConsToClose(idleCloseCount));
        	if (readyCloseCons.size() >= idleCloseCount) {
        		break;
        	}
        }

        for (BackendConnection idleCon : readyCloseCons) {
        	if (idleCon.isBorrowed()) {
        		log.warn("Find idle connection is using: {}", idleCon);
        	}
        	idleCon.close("Too many idle connection");
        }
    }

    private void createByIdleLittle(int idleCons, int createCount) {
        log.debug("Create connections, because idle connection not enough, cur is {}, minCon is {} for '{}'",
                idleCons, hostConfig.getMinCon(), this.name);
        NewConnectionRespHandler simpleHandler = new NewConnectionRespHandler();
        final String[] schemas = dbPool.getSchemas();

        for (int i = 0; i < createCount; i++) {
        	if (this.getActiveCount() + this.getIdleCount() >= size) {
        		break;
        	}
			if (schemas.length == 0) {
				log.warn("No schema reference in host '{}'", this.name);
				return;
			}
			createNewConnection(null, simpleHandler, null, schemas[i % schemas.length]);
        }
    }

	public int getActiveCount() {
		return this.conMap.getActiveCountForDs(this);
	}

	public void clearCons(String reason) {
		this.conMap.clearConnections(reason, this);
	}

	public void startHeartbeat() {
		this.heartbeat.start();
	}

	public void stopHeartbeat() {
		heartbeat.stop();
	}

	public void heartbeat() {
		// 未到预定恢复时间，不执行心跳检测。
		if (TimeUtil.currentTimeMillis() < this.heartbeatRecoveryTime) {
			log.debug("Skip heartbeat: recovery time not reached in dataSource '{}'", this.name);
			return;
		}

		if (!this.heartbeat.isStop()) {
			try {
				this.heartbeat.heartbeat();
			} catch (Exception e) {
				log.error(this.name + " heartbeat failed", e);
			}
		}
	}

	private BackendConnection takeCon(BackendConnection conn, final ResponseHandler handler,
									  final Object attachment, String schema, boolean isNew) {
		NioProcessor processor = NioProcessor.ensureRunInProcessor();

		conn.setBorrowed(true);
		if (!conn.getSchema().equals(schema)) {
			// need do schema syn in before sql send
			conn.setSchema(schema);
		}
		ConQueue queue = this.conMap.getSchemaConQueue(schema);
		queue.incExecuteCount();
		conn.setAttachment(attachment);
        // 每次取连接的时候，更新下lastTime，防止在前端连接检查的时候，关闭连接，导致sql执行失败
		conn.setLastTime(System.currentTimeMillis());

		if (!isNew) {
			// Note: new connection has been registered when finishConnect() in processor,
			// here register again for binding connection into current processor.
			processor.register(conn, false);
		}
		handler.connectionAcquired(conn);
		return conn;
	}

	private void createNewConnection(RouteResultsetNode rrs, final ResponseHandler handler,
			final Object attachment, final String schema) {

		final PhysicalDataSource self = this;
		Runnable connectTask = new Runnable() {
			public void run() {
				try {
					ResponseHandler delegateHandler = new DelegateResponseHandler(handler) {
						@Override
						public void connectionAcquired(BackendConnection conn) {
							log.debug(">> Acquire a new backend {}", conn);
							self.takeCon(conn, super.target, attachment, schema, true);
						}
					};
					self.createNewConnection(delegateHandler, schema);
				} catch (IOException e) {
					handler.connectionError(e, null);
				}
			}
		};

		NioProcessor.runInProcessor(connectTask);
	}

    public void getConnection(final String schema, boolean autocommit, RouteResultsetNode rrs,
							  final ResponseHandler handler, final Object attachment) {
		final PhysicalDataSource self = this;
		final BackendConnection con = this.conMap.tryTakeCon(schema, autocommit);
        if (con != null) {
        	Runnable executeTask = new Runnable() {
				@Override
				public void run() {
					log.debug(">> Acquire a pooled backend {}", con);
					self.takeCon(con, handler, attachment, schema, false);
				}
			};
			NioProcessor.runInProcessor(executeTask);
        } else {
			// Check connections constraint: active <= max
            int activeCons = getActiveCount();
            if (activeCons + 1 > this.size) {
            	// Next active connections bigger than max connections limit
            	String s = "Max connections limit exceeded in backend pool '" + this.name + "'";
				Throwable cause = new IOException(s);
				log.debug(s);
                handler.connectionError(cause, null);
            } else {
                log.debug("No idle connection, create a new for schema '{}' in pool '{}'",
						schema, this.name);
                createNewConnection(rrs, handler, attachment, schema);
            }
        }
    }

	private void returnCon(BackendConnection c) {
		c.setAttachment(null);
		c.setBorrowed(false);
		c.setLastTime(TimeUtil.currentTimeMillis());
		ConQueue queue = this.conMap.getSchemaConQueue(c.getSchema());

		final boolean ok;
		if (c.isAutocommit()) {
			ok = queue.getAutoCommitCons().offer(c);
		} else {
			ok = queue.getManCommitCons().offer(c);
		}
		if (!ok) {
			log.warn("Can't return to pool, so close backend {}", c);
			c.close("Pool full");
		}
	}

	public void releaseChannel(BackendConnection c) {
        log.debug("<< Release a backend {}", c);
		// release connection
		returnCon(c);
	}

	public void connectionClosed(BackendConnection conn) {
		ConQueue queue = this.conMap.getSchemaConQueue(conn.getSchema());
		if (queue != null) {
			queue.removeCon(conn);
		}
	}

	public abstract void createNewConnection(ResponseHandler handler, String schema)
			throws IOException;

	public long getHeartbeatRecoveryTime() {
		return heartbeatRecoveryTime;
	}

	public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
		this.heartbeatRecoveryTime = heartbeatRecoveryTime;
	}

	public DBHostConfig getConfig() {
		return config;
	}

}
