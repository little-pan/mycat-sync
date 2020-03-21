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
package org.opencloudb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.log4j.Logger;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.buffer.BufferPool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.classloader.DynaClassLoader;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.interceptor.SQLInterceptor;
import org.opencloudb.manager.ManagerConnectionFactory;
import org.opencloudb.net.*;
import org.opencloudb.route.MyCATSequnceProcessor;
import org.opencloudb.route.RouteService;
import org.opencloudb.server.ServerConnectionFactory;
import org.opencloudb.statistic.SQLRecorder;
import org.opencloudb.util.ExecutorUtil;
import org.opencloudb.util.IoUtil;
import org.opencloudb.util.NameableExecutor;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public class MycatServer {
	private static final Logger log = Logger.getLogger(MycatServer.class);

	public static final String NAME = "MyCat";
	private static final long LOG_WATCH_DELAY = 60000L;
	private static final long TIME_UPDATE_PERIOD = 20L;
	private static final MycatServer INSTANCE = new MycatServer();

	private final RouteService routerService;
	private final CacheService cacheService;
	private Properties dnIndexProperties;
	private AsynchronousChannelGroup[] asyncChannelGroups;
	private volatile int channelIndex = 0;
	private final MyCATSequnceProcessor sequnceProcessor = new MyCATSequnceProcessor();
	private final DynaClassLoader catletClassLoader;
	private final SQLInterceptor sqlInterceptor;
	private volatile int nextProcessor;
	private BufferPool bufferPool;
	private boolean aio = false;
	private final AtomicLong xaIDInc = new AtomicLong();

	public static final MycatServer getInstance() {
		return INSTANCE;
	}

	private final MycatConfig config;
	private final Timer timer;
	private final SQLRecorder sqlRecorder;
	private final AtomicBoolean isOnline;
	private final long startupTime;
	private ConnectionManager connectionManager;
	private SocketConnector connector;
	private BioProcessorPool processorPool;
	private NameableExecutor businessExecutor;
	private NameableExecutor timerExecutor;
	private ListeningExecutorService listeningExecutorService;

	public MycatServer() {
		this.config = new MycatConfig();
		this.timer = new Timer(NAME + "Timer", true);
		this.sqlRecorder = new SQLRecorder(config.getSystem().getSqlRecordCount());
		this.isOnline = new AtomicBoolean(true);
		cacheService = new CacheService();
		routerService = new RouteService(cacheService);
		// load datanode active index from properties
		dnIndexProperties = loadDnIndexProps();
		try {
			sqlInterceptor = (SQLInterceptor) Class.forName(
					config.getSystem().getSqlInterceptor()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		catletClassLoader = new DynaClassLoader(SystemConfig.getHomePath()
				+ File.separator + "catlet", config.getSystem()
				.getCatletClassCheckSeconds());
		this.startupTime = TimeUtil.currentTimeMillis();
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public NameableExecutor getTimerExecutor() {
		return timerExecutor;
	}

	public DynaClassLoader getCatletClassLoader() {
		return catletClassLoader;
	}

	public MyCATSequnceProcessor getSequnceProcessor() {
		return sequnceProcessor;
	}

	public SQLInterceptor getSqlInterceptor() {
		return sqlInterceptor;
	}

	public String genXATXID() {
		long seq = this.xaIDInc.incrementAndGet();
		if (seq < 0) {
			synchronized (xaIDInc) {
				if (xaIDInc.get() < 0) {
					xaIDInc.set(0);
				}
				seq = xaIDInc.incrementAndGet();
			}
		}
		return "'Mycat." + this.getConfig().getSystem().getMycatNodeId() + "."
				+ seq+"'";
	}

	/**
	 * get next AsynchronousChannel ,first is exclude if multi
	 * AsynchronousChannelGroups
	 * 
	 * @return
	 */
	public AsynchronousChannelGroup getNextAsyncChannelGroup() {
		if (asyncChannelGroups.length == 1) {
			return asyncChannelGroups[0];
		} else {
			int index = (++channelIndex) % asyncChannelGroups.length;
			if (index == 0) {
				++channelIndex;
				return asyncChannelGroups[1];
			} else {
				return asyncChannelGroups[index];
			}

		}
	}

	public MycatConfig getConfig() {
		return config;
	}

	public void beforeStart() {
		String home = SystemConfig.getHomePath();
		Log4jInitializer.configureAndWatch(home + "/conf/log4j.xml", LOG_WATCH_DELAY);
		
		//ZkConfig.instance().initZk();
	}

	public void startup() throws IOException {
        int initExecutor = Runtime.getRuntime().availableProcessors() * 2;
        initExecutor = Math.min(initExecutor, 10);

		SystemConfig system = config.getSystem();
		final int processorCount = system.getProcessors();
        initExecutor = Math.min(initExecutor, processorCount);

		// server startup
        log.info("===============================================");
        log.info(NAME + " is ready to startup ...");
		String inf = "Startup processors ...,total processors:"
				+ system.getProcessors() + ", business executor:"
				+ system.getProcessorExecutor()
				+ "    \r\n buffer chunk size:"
				+ system.getProcessorBufferChunk()
				+ "  buffer pool capacity(bufferPool / bufferChunk) is:"
				+ system.getProcessorBufferPool() / system.getProcessorBufferChunk();
        log.info(inf);
        log.info("Sysconfig params:" + system.toString());

		// startup processors
		final int threadPoolSize = system.getProcessorExecutor();
		long processBuferPool = system.getProcessorBufferPool();
		int processBufferChunk = system.getProcessorBufferChunk();
		int socketBufferLocalPercent = system.getProcessorBufferLocalPercent();
        this.bufferPool = new BufferPool(processBuferPool, processBufferChunk,
				        socketBufferLocalPercent / processorCount);
        this.businessExecutor = ExecutorUtil.create("BusinessExecutor", initExecutor, threadPoolSize);
        this.connectionManager = new ConnectionManager("ConnectionMgr",
                                                this.bufferPool, this.businessExecutor);
        this.timerExecutor = ExecutorUtil.create("Timer", system.getTimerExecutor());
        this.listeningExecutorService = MoreExecutors.listeningDecorator(businessExecutor);

		if (this.aio = (system.getUsingAIO() == 1)) {
            log.warn("aio network handler deprecated and ignore");
		}
        log.info("using bio network handler");
        // startup manager and server
        ManagerConnectionFactory mf = new ManagerConnectionFactory();
        ServerConnectionFactory sf = new ServerConnectionFactory();
        BioAcceptor manager = null, server = null;
        this.connector = new BioConnector( this.businessExecutor);
		String prefix = BufferPool.LOCAL_BUF_THREAD_PREX + "BioProcessorPool";
        this.processorPool = new BioProcessorPool(prefix, initExecutor, processorCount, false);
        boolean failed = true;
        try {
            manager = new BioAcceptor(NAME + "Manager", system.getBindIp(), system.getManagerPort(),
                    mf, this.processorPool);
            server  = new BioAcceptor(NAME + "Server",  system.getBindIp(), system.getServerPort(),
                    sf, this.processorPool);
            manager.start();
            server.start();
            log.info("===============================================");

            // init datahost
            Map<String, PhysicalDBPool> dataHosts = config.getDataHosts();
            log.info("Initialize dataHost ...");
            for (PhysicalDBPool node : dataHosts.values()) {
                String index = dnIndexProperties.getProperty(node.getHostName(),
                        "0");
                if (!"0".equals(index)) {
                    log.info("init datahost: " + node.getHostName()
                            + "  to use datasource index:" + index);
                }
                node.init(Integer.valueOf(index));
                node.startHeartbeat();
            }
            long dataNodeIldeCheckPeriod = system.getDataNodeIdleCheckPeriod();
            this.timer.schedule(updateTime(), 0L, TIME_UPDATE_PERIOD);
            this.timer.schedule(createConnectionCheckTask(), 0L, system.getProcessorCheckPeriod());
            this.timer.schedule(dataNodeConHeartBeatCheck(dataNodeIldeCheckPeriod), 0L, dataNodeIldeCheckPeriod);
            this.timer.schedule(dataNodeHeartbeat(), 0L, system.getDataNodeHeartbeatPeriod());
            this.timer.schedule(catletClassClear(), 30000);
            failed = false;
        } finally {
            if (failed) {
                IoUtil.close(manager);
                IoUtil.close(server);
                IoUtil.close(this.processorPool);
                this.processorPool.join();
            }
        }
	}

	private TimerTask catletClassClear() {
		return new TimerTask() {
			@Override
			public void run() {
				try {
					catletClassLoader.clearUnUsedClass();
				} catch (Exception e) {
                    log.warn("catletClassClear err " + e);
				}
			};
		};
	}

	private Properties loadDnIndexProps() {
		Properties prop = new Properties();
		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
		if (!file.exists()) {
			return prop;
		}
		FileInputStream filein = null;
		try {
			filein = new FileInputStream(file);
			prop.load(filein);
		} catch (Exception e) {
            log.warn("load DataNodeIndex err:" + e);
		} finally {
			if (filein != null) {
				try {
					filein.close();
				} catch (IOException e) {
				}
			}
		}
		return prop;
	}

	/**
	 * save cur datanode index to properties file
	 * 
	 * @param dataHost
	 * @param curIndex
	 */
	public synchronized void saveDataHostIndex(String dataHost, int curIndex) {

		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
		FileOutputStream fileOut = null;
		try {
			String oldIndex = dnIndexProperties.getProperty(dataHost);
			String newIndex = String.valueOf(curIndex);
			if (newIndex.equals(oldIndex)) {
				return;
			}
			dnIndexProperties.setProperty(dataHost, newIndex);
            log.info("save DataHost index  " + dataHost + " cur index " + curIndex);

			File parent = file.getParentFile();
			if (parent != null && !parent.exists()) {
				parent.mkdirs();
			}

			fileOut = new FileOutputStream(file);
			dnIndexProperties.store(fileOut, "update");
		} catch (Exception e) {
            log.warn("saveDataNodeIndex err:", e);
		} finally {
			if (fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
				}
			}
		}

	}

	public RouteService getRouterService() {
		return routerService;
	}

	public CacheService getCacheService() {
		return cacheService;
	}

	public NameableExecutor getBusinessExecutor() {
		return businessExecutor;
	}

	public RouteService getRouterservice() {
		return routerService;
	}

	public ConnectionManager getConnectionManager() {
		return this.connectionManager;
	}

	public SocketConnector getConnector() {
		return connector;
	}

	public SQLRecorder getSqlRecorder() {
		return sqlRecorder;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public boolean isOnline() {
		return isOnline.get();
	}

	public void offline() {
		isOnline.set(false);
	}

	public void online() {
		isOnline.set(true);
	}

	// 系统时间定时更新任务
	private TimerTask updateTime() {
		return new TimerTask() {
			@Override
			public void run() {
				TimeUtil.update();
			}
		};
	}

	protected void checkBackendCons() {
	    try {
            this.connectionManager.checkBackendCons();
        } catch (Exception e) {
            log.warn("checkBackendCons caught error", e);
        }
    }

    protected void checkFrontCons() {
        try {
            this.connectionManager.checkFrontCons();
        } catch (Exception e) {
            log.warn("checkFrontCons caught error", e);
        }
    }

	private TimerTask createConnectionCheckTask() {
	    final NameableExecutor executor = this.timerExecutor;
		return new TimerTask() {
			@Override
			public void run() {
                executor.execute(new Runnable() {
					@Override
					public void run() {
                        checkBackendCons();
					}
				});
                executor.execute(new Runnable() {
					@Override
					public void run() {
                        checkFrontCons();
					}
				});
			}
		};
	}

	// 数据节点定时连接空闲超时检查任务
	private TimerTask dataNodeConHeartBeatCheck(final long heartPeriod) {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						Map<String, PhysicalDBPool> nodes = config
								.getDataHosts();
						for (PhysicalDBPool node : nodes.values()) {
							node.heartbeatCheck(heartPeriod);
						}
						Map<String, PhysicalDBPool> _nodes = config
								.getBackupDataHosts();
						if (_nodes != null) {
							for (PhysicalDBPool node : _nodes.values()) {
								node.heartbeatCheck(heartPeriod);
							}
						}
					}
				});
			}
		};
	}

	// 数据节点定时心跳任务
	private TimerTask dataNodeHeartbeat() {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						Map<String, PhysicalDBPool> nodes = config
								.getDataHosts();
						for (PhysicalDBPool node : nodes.values()) {
							node.doHeartbeat();
						}
					}
				});
			}
		};
	}

	public boolean isAIO() {
		return aio;
	}

	public ListeningExecutorService getListeningExecutorService() {
		return listeningExecutorService;
	}
}