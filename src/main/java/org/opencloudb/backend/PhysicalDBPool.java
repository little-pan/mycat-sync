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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import org.opencloudb.MycatServer;
import org.opencloudb.config.Alarms;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.mysql.nio.handler.GetConnectionHandler;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.route.RouteResultsetNode;
import org.slf4j.*;

/**
 * A physical represent of dataHost.
 */
public class PhysicalDBPool {
	
	static final Logger log = LoggerFactory.getLogger(PhysicalDBPool.class);
	
	public static final int BALANCE_NONE = 0;
	public static final int BALANCE_ALL_BACK = 1;
	public static final int BALANCE_ALL = 2;
    public static final int BALANCE_ALL_READ = 3;
    
	public static final int WRITE_ONLYONE_NODE = 0;
	public static final int WRITE_RANDOM_NODE = 1;
	public static final int WRITE_ALL_NODE = 2;
	
	public static final long LONG_TIME = 300000;
	public static final int WEIGHT = 0;

	private final String hostName;
	
	protected PhysicalDatasource[] writeSources;
	protected Map<Integer, PhysicalDatasource[]> readSources;
	
	protected volatile int activedIndex;
	protected volatile boolean initSuccess;
	
	protected final ReentrantLock switchLock = new ReentrantLock();
	private final Collection<PhysicalDatasource> allDs;
	private final int balance;
	private final int writeType;
	private String[] schemas;
	private final DataHostConfig dataHostConfig;

	public PhysicalDBPool(String name, DataHostConfig conf,
			PhysicalDatasource[] writeSources,
			Map<Integer, PhysicalDatasource[]> readSources, int balance,
			int writeType) {
		this.hostName = name;
		this.dataHostConfig = conf;
		this.writeSources = writeSources;
		this.balance = balance;
		this.writeType = writeType;
		
		Iterator<Map.Entry<Integer, PhysicalDatasource[]>> it = readSources.entrySet().iterator();
		while (it.hasNext()) {
			PhysicalDatasource[] values = it.next().getValue();
			if (values.length == 0) {
				it.remove();
			}
		}
		
		this.readSources = readSources;
		this.allDs = this.genAllDataSources();
		log.info("Total resources of dataHost '{}': {}", this.hostName,  allDs.size());
		
		setDataSourceProps();
	}

	public int getWriteType() {
		return writeType;
	}

	private void setDataSourceProps() {
		for (PhysicalDatasource ds : this.allDs) {
			ds.setDbPool(this);
		}
	}

	public PhysicalDatasource findDatasouce(BackendConnection exitsCon) {
		for (PhysicalDatasource ds : this.allDs) {
			if (ds.isReadNode() == exitsCon.isFromSlaveDB()) {
				if (ds.isMyConnection(exitsCon)) {
					return ds;
				}
			}
		}

		log.warn("Can't find connection in pool {} for con: {}",  this.hostName, exitsCon);
		return null;
	}

	public String getHostName() {
		return hostName;
	}

	/**
	 * all write datanodes
	 * @return
	 */
	public PhysicalDatasource[] getSources() {
		return writeSources;
	}
	
	public PhysicalDatasource getSource() {
		switch (this.writeType) {
			case WRITE_ONLYONE_NODE: {
				PhysicalDatasource result = this.writeSources[this.activedIndex];
				log.debug("select actived write source '{}' for dataHost '{}'", result.getName(), getHostName());
				return result;
			}
			case WRITE_RANDOM_NODE: {
				Random random = ThreadLocalRandom.current();
				int count = this.writeSources.length;
				int index = Math.abs(random.nextInt()) % count;
				PhysicalDatasource result = this.writeSources[index];
				if (!isAlive(result)) {
					// Find all live nodes
					List<Integer> alives = new ArrayList<>(count - 1);
					for (int i = 0; i < count; i++) {
						if (i != index) {
							if (isAlive(this.writeSources[i])) {
								alives.add(i);
							}
						}
					}
					
					if (alives.isEmpty()) {
						result = this.writeSources[0];
					} else {
						// random select one
						index = Math.abs(random.nextInt()) % alives.size();
						result = this.writeSources[alives.get(index)];
					}
				}

                log.debug("select random write source '{}' for dataHost '{}'", result.getName(), getHostName());
				return result;
			}
			default: {
				throw new java.lang.IllegalArgumentException("writeType is "
						+ this.writeType + ", so can't return one write dataSource ");
			}
		}

	}

	public int getActivedIndex() {
		return this.activedIndex;
	}

	public boolean isInitSuccess() {
		return this.initSuccess;
	}

	public int next(int i) {
		if (checkIndex(i)) {
			return (++i == writeSources.length) ? 0 : i;
		} else {
			return 0;
		}
	}

	public boolean switchSource(int newIndex, boolean isAlarm, String reason) {
		if (this.writeType != PhysicalDBPool.WRITE_ONLYONE_NODE || !checkIndex(newIndex)) {
			return false;
		}
		
		final ReentrantLock lock = this.switchLock;
		lock.lock();
		try {
			int current = activedIndex;
			if (current != newIndex) {
				// switch index
				this.activedIndex = newIndex;
				// init again
				this.init(activedIndex);
				
				// clear all connections
				this.getSources()[current].clearCons("switch datasource");
				
				// write log
				log.warn(switchMessage(current, newIndex, false, reason));
				return true;
			}
		} finally {
			lock.unlock();
		}
		return false;
	}

	private String switchMessage(int current, int newIndex, boolean alarm, String reason) {
		StringBuilder s = new StringBuilder();
		if (alarm) {
			s.append(Alarms.DATANODE_SWITCH);
		}
		s.append("[Host=").append(hostName).append(",result=[").append(current).append("->");
		s.append(newIndex).append("],reason=").append(reason).append(']');
		return s.toString();
	}

	private int loop(int i) {
		return i < writeSources.length ? i : (i - writeSources.length);
	}

	public void init(int index) {
		if (!checkIndex(index)) {
			index = 0;
		}
		
		int active = -1;
		for (int i = 0; i < this.writeSources.length; i++) {
			int j = loop(i + index);
			if (initSource(j, this.writeSources[j])) {
                // 不切换-1时，如果主写挂了不允许切换过去
                if (this.dataHostConfig.getSwitchType() == DataHostConfig.NOT_SWITCH_DS && j > 0) {
                   break;
                }

				active = j;
				this.activedIndex = active;
				this.initSuccess = true;
				log.info(getMessage(active, " init success"));

				if (this.writeType == WRITE_ONLYONE_NODE) {
					// only init one write dataSource
					MycatServer.getInstance().saveDataHostIndex(this.hostName, this.activedIndex);
					break;
				}
			}
		}
		
		if (!checkIndex(active)) {
			this.initSuccess = false;
			StringBuilder s = new StringBuilder();
			s.append(Alarms.DEFAULT).append(hostName).append(" init failure");
			log.error(s.toString());
		}
	}

	private boolean checkIndex(int i) {
		return i >= 0 && i < this.writeSources.length;
	}

	private String getMessage(int index, String info) {
		return new StringBuilder().append(this.hostName).append(" index:").append(index).append(info) + "";
	}

	private boolean initSource(int index, PhysicalDatasource ds) {
		final int initSize = ds.getConfig().getMinCon();
		log.info("Init backend source, create connections total {} for '{}' index {}" ,
				initSize, ds.getName(), index);
		if (this.schemas.length == 0) {
			log.warn("No schema reference dataHost '{}'", this.hostName);
			return false;
		}

		// Note: connection needs to be done once at least for init dataSource, even though initSize <= 0!
		int i = 0, total = Math.max(initSize, 1);
		GetConnectionHandler getConHandler = new GetConnectionHandler(total);
		do {
			try {
				String schema = this.schemas[i % this.schemas.length];
				ds.getConnection(schema, true, null, getConHandler, null);
			} catch (Throwable cause) {
				getConHandler.connectionError(cause, null);
			}
		} while (++i < initSize);

		// waiting for finish
		int waitSeconds = ds.getInitSourceWaitSeconds();
		log.debug("Wait for init connections: max wait {} seconds", waitSeconds);
		if (waitSeconds <= 0) {
			getConHandler.await();
		} else {
			getConHandler.await(waitSeconds, TimeUnit.SECONDS);
		}
		log.debug("Init result: {}", getConHandler.getStatusInfo());

		return (getConHandler.getSuccessCons() > 0);
	}

	public void heartbeat() {
		if (this.writeSources == null || this.writeSources.length == 0) {
			log.debug("Skip heartbeat: no write dataSource configured in dataHost '{}'", this.hostName);
			return;
		}

		for (PhysicalDatasource ds: this.allDs) {
			ds.heartbeat();
		}
	}

	/**
	 * Backend physical connection heartbeat check
	 */
	public void heartbeatCheck(long idleCheckPeriod) {
		for (PhysicalDatasource ds : this.allDs) {
			// only read node or all write node or writetype=WRITE_ONLYONE_NODE
			// and current write node will check
			if (ds != null
					&& (ds.getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS)
					&& (ds.isReadNode()
							|| (this.writeType != WRITE_ONLYONE_NODE)
							|| (ds == this.getSource()))) {
				ds.heatBeatCheck(ds.getConfig().getIdleTimeout(), idleCheckPeriod);
			}
		}
	}

	public void startHeartbeat() {
		for (PhysicalDatasource source : this.allDs) {
			source.startHeartbeat();
		}
	}

	public void stopHeartbeat() {
		for (PhysicalDatasource source : this.allDs) {
			source.stopHeartbeat();
		}
	}

	public void clearDataSources(String reason) {
		log.info("clear dataSource of pool {}", this.hostName);
		for (PhysicalDatasource source : this.allDs) {			
			log.info("clear dataSource of pool {} ds: {}", this.hostName, source.getConfig());
			source.clearCons(reason);
			source.stopHeartbeat();
		}
	}

	public Collection<PhysicalDatasource> genAllDataSources() {
		LinkedList<PhysicalDatasource> allSources = new LinkedList<PhysicalDatasource>();
		for (PhysicalDatasource ds : writeSources) {
			if (ds != null) {
				allSources.add(ds);
			}
		}
		
		for (PhysicalDatasource[] dataSources : this.readSources.values()) {
			for (PhysicalDatasource ds : dataSources) {
				if (ds != null) {
					allSources.add(ds);
				}
			}
		}
		return allSources;
	}

	public Collection<PhysicalDatasource> getAllDataSources() {
		return this.allDs;
	}

	/**
	 * return connection for read balance
	 *
	 * @param handler
	 * @param attachment
	 * @param database
	 * @throws Exception
	 */
	public void getRWBalanceCon(String schema, boolean autocommit, RouteResultsetNode rrs,
			ResponseHandler handler, Object attachment, String database) throws Exception {
		PhysicalDatasource theNode = null;
		ArrayList<PhysicalDatasource> okSources = null;
		switch (this.balance) {
		case BALANCE_ALL_BACK: {			
			// all read nodes and the standard by masters
			okSources = getAllActiveRWSources(true, false, checkSlaveSynStatus());
			if (okSources.isEmpty()) {
				theNode = this.getSource();
			} else {
				theNode = randomSelect(okSources);
			}
			break;
		}
		case BALANCE_ALL: {
			okSources = getAllActiveRWSources(true, true, checkSlaveSynStatus());
			theNode = randomSelect(okSources);
			break;
		}
        case BALANCE_ALL_READ: {
            okSources = getAllActiveRWSources(false, false, checkSlaveSynStatus());
            theNode = randomSelect(okSources);
            break;
        }
		case BALANCE_NONE:
		default:
			// return default write data source
			theNode = this.getSource();
		}

        log.debug("select read node '{}' in dataHost '{}'",  theNode.getName(), this.getHostName());
		theNode.getConnection(schema, autocommit, rrs, handler, attachment);
	}

	private boolean checkSlaveSynStatus() {
		return ( dataHostConfig.getSlaveThreshold() != -1 )
				&& (dataHostConfig.getSwitchType() == DataHostConfig.SYN_STATUS_SWITCH_DS);
	}

	
	/**
	 * TODO: modify by zhuam
	 * 
	 * 随机选择，按权重设置随机概率。
     * 在一个截面上碰撞的概率高，但调用量越大分布越均匀，而且按概率使用权重后也比较均匀，有利于动态调整提供者权重。
	 * @param okSources
	 * @return a random dataSource
	 */
	public PhysicalDatasource randomSelect(ArrayList<PhysicalDatasource> okSources) {
		if (okSources.isEmpty()) {
			return this.getSource();
		} else {
			int length = okSources.size(); 	// 总个数
	        int totalWeight = 0; 			// 总权重
	        boolean sameWeight = true; 		// 权重是否都一样
	        for (int i = 0; i < length; i++) {	        	
	            int weight = okSources.get(i).getConfig().getWeight();
	            totalWeight += weight; 		// 累计总权重	            
	            if (sameWeight && i > 0 
	            		&& weight != okSources.get(i-1).getConfig().getWeight() ) {	  // 计算所有权重是否一样          		            	
	                sameWeight = false; 	
	            }
	        }

	        Random random = ThreadLocalRandom.current();
	        if (totalWeight > 0 && !sameWeight ) {
	        	// 如果权重不相同且权重大于0则按总权重数随机
	            int offset = random.nextInt(totalWeight);
	            // 并确定随机值落在哪个片断上
	            for (int i = 0; i < length; i++) {
	                offset -= okSources.get(i).getConfig().getWeight();
	                if (offset < 0) {
	                    return okSources.get(i);
	                }
	            }
	        }
	        
	        // 如果权重相同或权重为0则均等随机
	        return okSources.get( random.nextInt(length) );
		}
	}

    public int getBalance() {
        return balance;
    }
    
	private boolean isAlive(PhysicalDatasource source) {
		return (source.getHeartbeat().getStatus() == DBHeartbeat.OK_STATUS);
	}

	private boolean canSelectAsReadNode(PhysicalDatasource theSource) {
        if(theSource.getHeartbeat().getSlaveBehindMaster() == null
                			||theSource.getHeartbeat().getDbSynStatus() == DBHeartbeat.DB_SYN_ERROR){
            return false;
        }
        
		return (theSource.getHeartbeat().getDbSynStatus() == DBHeartbeat.DB_SYN_NORMAL)
				&& (theSource.getHeartbeat().getSlaveBehindMaster() < this.dataHostConfig.getSlaveThreshold());
	}

	/**
     * return all backup write sources
     * 
     * @param includeWriteNode if include write nodes
     * @param includeCurWriteNode if include current active write node.
	 *                            invalid when <code>includeWriteNode<code> is false
     * @param filterWithSlaveThreshold
     *
     * @return
     */
	private ArrayList<PhysicalDatasource> getAllActiveRWSources(
    		boolean includeWriteNode, boolean includeCurWriteNode, boolean filterWithSlaveThreshold) {
		
		int curActive = activedIndex;
		ArrayList<PhysicalDatasource> okSources = new ArrayList<PhysicalDatasource>(this.allDs.size());
		
		for (int i = 0; i < this.writeSources.length; i++) {
			PhysicalDatasource theSource = writeSources[i];
			if (isAlive(theSource)) {// write node is active
                
				if (includeWriteNode) {
					if (i == curActive && !includeCurWriteNode) {
						// not include cur active source
					} else if (filterWithSlaveThreshold) {
						if (canSelectAsReadNode(theSource)) {
							okSources.add(theSource);
						} else {
							continue;
						}
					} else {
						okSources.add(theSource);
					}
                }
                
				if (!readSources.isEmpty()) {
					// check all slave nodes
					PhysicalDatasource[] allSlaves = this.readSources.get(i);
					if (allSlaves != null) {
						for (PhysicalDatasource slave : allSlaves) {
							if (isAlive(slave)) {
								if (filterWithSlaveThreshold) {									
									if (canSelectAsReadNode(slave)) {
										okSources.add(slave);
									} else {
										continue;
									}
								} else {
									okSources.add(slave);
								}
							}
						}
					}
				}
			} else {
				// TODO : add by zhuam	
			    // 如果写节点不OK, 也要保证临时的读服务正常
				if ( this.dataHostConfig.isTempReadHostAvailable() ) {
					if (!readSources.isEmpty()) {
						// check all slave nodes
						PhysicalDatasource[] allSlaves = this.readSources.get(i);
						if (allSlaves != null) {
							for (PhysicalDatasource slave : allSlaves) {
								if (isAlive(slave)) {
									
									if (filterWithSlaveThreshold) {									
										if (canSelectAsReadNode(slave)) {
											okSources.add(slave);
										} else {
											continue;
										}
									} else {
										okSources.add(slave);
									}
								}
							}
						}
					}
				}				
			}

		}
		return okSources;
	}

    public String[] getSchemas() {
		return schemas;
	}

	public void setSchemas(String[] mySchemas) {
		this.schemas = mySchemas;
	}

}