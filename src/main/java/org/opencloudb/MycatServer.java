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
import org.opencloudb.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public class MycatServer {

	static final Logger log = LoggerFactory.getLogger(MycatServer.class);

	public static final String NAME = "MyCat";
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
		String catletDir = getDirectory("catlet");
		int checkSeconds = config.getSystem().getCatletClassCheckSeconds();
        this.catletClassLoader = new DynaClassLoader(catletDir, checkSeconds);
		this.startupTime = TimeUtil.currentTimeMillis();
	}

	public void beforeStart() {
		String home = SystemConfig.getHomePath();
        log.info("{}-{} starts in '{}'", NAME, ProcessUtil.getPid(), home);
	}

	public void startup() throws Exception {
        int initExecutor = Runtime.getRuntime().availableProcessors() * 2;
        initExecutor = Math.min(initExecutor, 10);

		SystemConfig system = config.getSystem();
		final int processorCount = system.getProcessors();
        initExecutor = Math.min(initExecutor, processorCount);

		// server startup
        log.info("=======================================================");
        log.info("{} is ready to startup ...", NAME);
        log.info("Total processors: {}, business executor: {}, buffer chunk size: {}, " +
                        "buffer pool capacity(bufferPool / bufferChunk) is: {}",
                system.getProcessors(), system.getProcessorExecutor(),
                system.getProcessorBufferChunk(), system.getProcessorBufferPool() / system.getProcessorBufferChunk());
        log.info("Sysconfig params: {}", system);

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
            log.warn("Aio network handler deprecated and ignore");
		}
        log.info("Startup manager and server: using bio network handler");
        BioAcceptor manager = null, server = null;
        boolean failed = true;
        try {
            this.connector = new BioConnector( this.businessExecutor);
            String prefix = BufferPool.LOCAL_BUF_THREAD_PREX + "BioProcessorPool";
            this.processorPool = new BioProcessorPool(prefix, initExecutor, processorCount, false);

			// init datahost
			Map<String, PhysicalDBPool> dataHosts = config.getDataHosts();
			log.info("Initialize dataHost ...");
			for (PhysicalDBPool node : dataHosts.values()) {
				String index = dnIndexProperties.getProperty(node.getHostName(),
						"0");
				if (!"0".equals(index)) {
					log.info("Init datahost: {}, use datasource index: {}", node.getHostName(), index);
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

            ManagerConnectionFactory mcFactory = new ManagerConnectionFactory();
            manager = new BioAcceptor(NAME + "Manager", system.getBindIp(), system.getManagerPort(),
                    mcFactory, this.processorPool);
            manager.start();
            manager.awaitStarted();

            ServerConnectionFactory scFactory = new ServerConnectionFactory();
            server  = new BioAcceptor(NAME + "Server",  system.getBindIp(), system.getServerPort(),
                    scFactory, this.processorPool);
            server.start();
            server.awaitStarted();

            log.info("=======================================================");
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

	private String getDirectory (String name) {
	    String home = SystemConfig.getHomePath();
	    return String.format("%s%s%s", home,  File.separator, name);
    }

	private File getConfigFile(String name) {
        String path = String.format("conf%s%s",  File.separator, name);
        String home = SystemConfig.getHomePath();
        return new File(home, path);
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
                        Map<String, PhysicalDBPool> nodes = config.getDataHosts();
                        for (PhysicalDBPool node : nodes.values()) {
                            node.doHeartbeat();
                        }
                    }
                });
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

    public BioProcessorPool getProcessorPool () {
        return this.processorPool;
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

    public MyCATSequnceProcessor getSequnceProcessor() {
        return sequnceProcessor;
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

	public boolean isAIO() {
		return aio;
	}

	public ListeningExecutorService getListeningExecutorService() {
		return listeningExecutorService;
	}

}