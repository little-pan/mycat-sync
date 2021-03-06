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
package org.opencloudb.response;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.opencloudb.MycatCluster;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.QuarantineConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.slf4j.*;

/**
 * @author mycat
 */
public final class RollbackConfig {
	private static final Logger log = LoggerFactory.getLogger(RollbackConfig.class);

	public static void execute(ManagerConnection c) {
		MycatServer server = MycatServer.getContextServer();
		final ReentrantLock lock = server.getConfig().getLock();
		lock.lock();
		try {
			if (rollback()) {
				StringBuilder s = new StringBuilder();
				log.warn("Rollback config success by manager in {}", c);
				OkPacket ok = new OkPacket();
				ok.packetId = 1;
				ok.affectedRows = 1;
				ok.serverStatus = 2;
				ok.message = "Rollback config success".getBytes();
				ok.write(c);
			} else {
				c.writeErrMessage(ErrorCode.ER_YES, "Rollback config failure");
			}
		} finally {
			lock.unlock();
		}
	}

	private static boolean rollback() {
		MycatServer server = MycatServer.getContextServer();
		MycatConfig conf = server.getConfig();
		Map<String, UserConfig> users = conf.getBackupUsers();
		Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
		Map<String, PhysicalDBNode> dataNodes = conf.getBackupDataNodes();
		Map<String, PhysicalDBPool> dataHosts = conf.getBackupDataHosts();
		MycatCluster cluster = conf.getBackupCluster();
		QuarantineConfig quarantine = conf.getBackupQuarantine();

		// 检查可回滚状态
		if (!conf.canRollback()) {
			return false;
		}

		// 如果回滚已经存在的pool
		boolean rollbackStatus = true;
		Map<String, PhysicalDBPool> cNodes = conf.getDataHosts();
		for (PhysicalDBPool dn : dataHosts.values()) {
			dn.init(dn.getActivedIndex());
			if (!dn.isInitSuccess()) {
				rollbackStatus = false;
				break;
			}
		}
		// 如果回滚不成功，则清理已初始化的资源。
		if (!rollbackStatus) {
			for (PhysicalDBPool dn : dataHosts.values()) {
				dn.clearDataSources("rollback config");
				dn.stopHeartbeat();
			}
			return false;
		}

		// 应用回滚
		conf.rollback(users, schemas, dataNodes, dataHosts, cluster, quarantine);

		// 处理旧的资源
		for (PhysicalDBPool dn : cNodes.values()) {
			dn.clearDataSources("clear old config");
			dn.stopHeartbeat();
		}

		//清理缓存
		server.getCacheService().clearCache();
		return true;
	}

}