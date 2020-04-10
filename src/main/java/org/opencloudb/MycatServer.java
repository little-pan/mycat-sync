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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.buffer.BufferPool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.classloader.DynaClassLoader;
import org.opencloudb.config.Isolations;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.interceptor.SQLInterceptor;
import org.opencloudb.manager.ManagerConnectionFactory;
import org.opencloudb.net.*;
import org.opencloudb.route.MyCATSequenceProcessor;
import org.opencloudb.route.RouteService;
import org.opencloudb.server.ServerConnectionFactory;
import org.opencloudb.statistic.SQLRecorder;
import org.opencloudb.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public class MycatServer {

	static final Logger log = LoggerFactory.getLogger(MycatServer.class);

	public static final String NAME = "Mycat";
	private static final long TIME_UPDATE_PERIOD = 20L;
    private static final ThreadLocal<MycatServer> CONTEXT_SERVER = new ThreadLocal<>();

	private final RouteService routerService;
	private final CacheService cacheService;
	private Properties dnIndexProperties;
	private final MyCATSequenceProcessor sequenceProcessor = new MyCATSequenceProcessor();
	private final DynaClassLoader catletClassLoader;
	private final SQLInterceptor sqlInterceptor;
	private BufferPool bufferPool;
    private final AtomicLong xaIDInc = new AtomicLong();

	private final MycatConfig config;
	private final ScheduledThreadPoolExecutor timer;
	private final SQLRecorder sqlRecorder;
	private final AtomicBoolean isOnline;
	private final long startupTime;

	private ConnectionManager connectionManager;
	private NioProcessorPool svrProcessorPool;
    private NioProcessorPool mgrProcessorPool;
	private NameableExecutor timerExecutor;

	public MycatServer() {
	    String prefix = NAME + "Timer-";
        ThreadFactory timerFactory = ExecutorUtil.createFactory(prefix, true);
		this.config = new MycatConfig();
        this.timer = new ScheduledThreadPoolExecutor(1, timerFactory);
		this.sqlRecorder = new SQLRecorder(this.config.getSystem().getSqlRecordCount());
		this.isOnline = new AtomicBoolean(true);
        this.cacheService = new CacheService();
        this.routerService = new RouteService(cacheService);
		// Load datanode active index from properties
        this.dnIndexProperties = loadDnIndexProps();
		try {
		    String interceptor = this.config.getSystem().getSqlInterceptor();
            this.sqlInterceptor = (SQLInterceptor) Class.forName(interceptor).newInstance();
		} catch (Exception e) {
			throw new ConfigException("Create sql interceptor error: " + e, e);
		}
		String catletDir = getDirectory("catlet");
		int checkSeconds = config.getSystem().getCatletClassCheckSeconds();
        this.catletClassLoader = new DynaClassLoader(catletDir, checkSeconds);
		this.startupTime = TimeUtil.currentTimeMillis();
	}

	public static MycatServer getContextServer() {
	    return CONTEXT_SERVER.get();
    }

    public static void setContextServer(MycatServer server) {
	    CONTEXT_SERVER.set(server);
    }

    public static void removeContextServer() {
        CONTEXT_SERVER.remove();
    }

	public void beforeStart() {
	    setContextServer(this);
	    try {
            String home = SystemConfig.getHomePath();
            log.info("{} pid {} bootstrap in '{}'", NAME, ProcessUtil.getPid(), home);
        } finally {
            removeContextServer();
        }
	}

	public void startup() throws Exception {
	    setContextServer(this);
	    try {
            SystemConfig system = this.config.getSystem();
            final int processorCount = system.getProcessors();

            // server startup
            log.info("=======================================================");
            log.info("{} is ready to startup ...", NAME);
            log.info("Total processors: {}, buffer chunk size: {}, " +
                            "buffer pool capacity(bufferPool / bufferChunk) is: {}",
                    system.getProcessors(), system.getProcessorBufferChunk(),
                    system.getProcessorBufferPool() / system.getProcessorBufferChunk());
            log.info("Transaction isolation level: {}", Isolations.getName(system.getTxIsolation()));
            log.info("Sysconfig params: {}", system);

            // startup processors
            long processBufferPool = system.getProcessorBufferPool();
            int processBufferChunk = system.getProcessorBufferChunk();
            int socketBufferLocalPercent = system.getProcessorBufferLocalPercent();
            this.bufferPool = new BufferPool(processBufferPool, processBufferChunk,
                    socketBufferLocalPercent / processorCount);
            this.connectionManager = new ConnectionManager("ConnectionMgr", this.bufferPool);
            this.timerExecutor = ExecutorUtil.create("TimerExecutor", system.getTimerExecutor());

            if (system.getUsingAIO() == 1) {
                log.warn("Aio network handler deprecated and ignore");
            }
            log.info("Startup manager and server: using nio synchronous network handler");
            NioAcceptor manager = null, server = null;
            boolean failed = true;
            try {
                String name = AbstractProcessor.PROCESSOR_THREAD_PREFIX + "Server";
                this.svrProcessorPool = new NioProcessorPool(name, processorCount);
                // Use an independent processor pool in manager for management isolation
                name = AbstractProcessor.PROCESSOR_THREAD_PREFIX + "Manager";
                this.mgrProcessorPool = new NioProcessorPool(name, 2);

                // init dataHost
                Map<String, PhysicalDBPool> dataHosts = config.getDataHosts();
                log.info("Initialize dataHost ...");
                for (PhysicalDBPool node : dataHosts.values()) {
                    String index = this.dnIndexProperties.getProperty(node.getHostName(), "0");
                    if (!"0".equals(index)) {
                        log.info("Init dataHost '{}', use dataSource index {}", node.getHostName(), index);
                    }
                    node.init(Integer.parseInt(index));
                    node.startHeartbeat();
                }
                // init and start timer
                startTimer();

                String bindIp = system.getBindIp();
                ManagerConnectionFactory mcFactory = new ManagerConnectionFactory();
                manager = new NioAcceptor(NAME + "Manager", bindIp, system.getManagerPort(),
                        mcFactory, this.mgrProcessorPool);
                manager.start();
                manager.awaitStarted();

                ServerConnectionFactory scFactory = new ServerConnectionFactory();
                server  = new NioAcceptor(NAME + "Server",  bindIp, system.getServerPort(),
                        scFactory, this.svrProcessorPool);
                server.start();
                server.awaitStarted();

                log.info("=======================================================");
                failed = false;
            } finally {
                if (failed) {
                    IoUtil.close(manager);
                    IoUtil.close(this.mgrProcessorPool);
                    if (this.mgrProcessorPool != null) this.mgrProcessorPool.join();
                    IoUtil.close(server);
                    IoUtil.close(this.svrProcessorPool);
                    if (this.svrProcessorPool != null) this.svrProcessorPool.join();
                }
            }
        } finally {
            removeContextServer();
        }
	}

	private void startTimer() {
	    SystemConfig system = this.config.getSystem();
        final long dataNodeIdleCheckPeriod = system.getDataNodeIdleCheckPeriod();
        final long initDelay = 0;

        Runnable task = updateTime();
        final TimeUnit millis = TimeUnit.MILLISECONDS;
        this.timer.scheduleWithFixedDelay(task, initDelay, TIME_UPDATE_PERIOD, millis);

        task = createConnectionCheckTask();
        this.timer.scheduleWithFixedDelay(task, initDelay, system.getProcessorCheckPeriod(), millis);

        task = dataNodeConHeartBeatCheck(dataNodeIdleCheckPeriod);
        this.timer.scheduleWithFixedDelay(task, initDelay, dataNodeIdleCheckPeriod, millis);

        task = dataNodeHeartbeat();
        this.timer.scheduleWithFixedDelay(task, initDelay, system.getDataNodeHeartbeatPeriod(), millis);

        task = catletClassClear();
        this.timer.schedule(task, 30000, millis);
    }

	private Runnable catletClassClear() {
		return new Runnable() {
			@Override
			public void run() {
				try {
                    log.debug("Clear catlet class");
					catletClassLoader.clearUnUsedClass();
				} catch (Exception e) {
                    log.warn("catletClassClear error ", e);
				}
			};
		};
	}

	private Properties loadDnIndexProps() {
		Properties prop = new Properties();
		File file = getConfigFile("dnindex.properties");
		if (!file.exists()) {
			return prop;
		}

		FileInputStream filein = null;
		try {
			filein = new FileInputStream(file);
			prop.load(filein);
		} catch (Exception e) {
            log.warn("load DataNodeIndex error", e);
		} finally {
		    IoUtil.close(filein);
		}

		return prop;
	}

	public static String getDirectory (String name) {
	    return SystemConfig.getDirectory(name);
    }

    public static File getConfigFile(String name) {
        return SystemConfig.getConfigFile(name);
    }

	/**
	 * save cur datanode index to properties file
	 * 
	 * @param dataHost
	 * @param curIndex
	 */
	public synchronized void saveDataHostIndex(String dataHost, int curIndex) {
		FileOutputStream fileOut = null;
		try {
			String oldIndex = this.dnIndexProperties.getProperty(dataHost);
			String newIndex = String.valueOf(curIndex);
			if (newIndex.equals(oldIndex)) {
				return;
			}
            this.dnIndexProperties.setProperty(dataHost, newIndex);
            log.info("save DataHost index {}, cur index {}", dataHost, curIndex);

            File file = getConfigFile("dnindex.properties");
			File parent = file.getParentFile();
			if (parent != null && !parent.exists()) {
				if (!parent.mkdirs()) {
				    throw new IOException("Can't create directory: '" + parent + "'");
                }
			}

			fileOut = new FileOutputStream(file);
            this.dnIndexProperties.store(fileOut, "update");
		} catch (Exception e) {
            log.warn("saveDataNodeIndex error", e);
		} finally {
			IoUtil.close(fileOut);
		}
	}

    private Runnable updateTime() {
        return new Runnable() {
            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    protected void checkBackendCons() {
        setContextServer(this);
        try {
            log.debug("Check backend connections");
            this.connectionManager.checkBackendCons();
        } catch (Exception e) {
            log.warn("checkBackendCons caught error", e);
        } finally {
            removeContextServer();
        }
    }

    protected void checkFrontCons() {
	    setContextServer(this);
        try {
            log.debug("Check frontend connections");
            this.connectionManager.checkFrontCons();
        } catch (Exception e) {
            log.warn("checkFrontCons caught error", e);
        } finally {
            removeContextServer();
        }
    }

    private Runnable createConnectionCheckTask() {
        final NameableExecutor executor = this.timerExecutor;
        return new Runnable() {
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

    // 数据节点定时连接空闲检查任务
    private Runnable dataNodeConHeartBeatCheck(final long heartPeriod) {
	    final MycatServer self = this;
        return new Runnable() {
            @Override
            public void run() {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        setContextServer(self);
                        try {
                            log.debug("The dataNode connection heartbeat runs");
                            Map<String, PhysicalDBPool> nodes = config.getDataHosts();
                            for (PhysicalDBPool node : nodes.values()) {
                                node.heartbeatCheck(heartPeriod);
                            }
                            Map<String, PhysicalDBPool> _nodes = config.getBackupDataHosts();
                            if (_nodes != null) {
                                for (PhysicalDBPool node : _nodes.values()) {
                                    node.heartbeatCheck(heartPeriod);
                                }
                            }
                        } finally {
                            removeContextServer();
                        }
                    }
                };

                self.timerExecutor.execute(task);
            }
        };
    }

    // 数据节点定时心跳任务
    private Runnable dataNodeHeartbeat() {
	    final MycatServer self = this;
        return new Runnable() {
            @Override
            public void run() {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        setContextServer(self);
                        try {
                            log.debug("The dataNode heartbeat runs");
                            Map<String, PhysicalDBPool> nodes = config.getDataHosts();
                            for (PhysicalDBPool node : nodes.values()) {
                                node.heartbeat();
                            }
                        } finally {
                            removeContextServer();
                        }
                    }
                };

                self.timerExecutor.execute(task);
            }
        };
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
        int nodeId = getConfig().getSystem().getMycatNodeId();
        return String.format("'%s.%d.%d'", "Mycat", nodeId, seq);
    }

    public NioProcessorPool getProcessorPool () {
        return this.svrProcessorPool;
    }

    public NioProcessorPool getManagerProcessorPool () {
        return this.mgrProcessorPool;
    }

    public MycatConfig getConfig() {
        return config;
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

    public MyCATSequenceProcessor getSequenceProcessor() {
        return this.sequenceProcessor;
    }

    public SQLInterceptor getSqlInterceptor() {
        return sqlInterceptor;
    }

	public RouteService getRouterService() {
		return routerService;
	}

	public CacheService getCacheService() {
		return cacheService;
	}

	public ConnectionManager getConnectionManager() {
		return this.connectionManager;
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

    public ThreadPoolExecutor getMycatTimer() {
	    return this.timer;
    }

}