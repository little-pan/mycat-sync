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
package org.opencloudb.cache;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.opencloudb.cache.impl.MapDBCachePooFactory;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.util.IoUtil;
import org.slf4j.*;

/**
 * Cache service for other component.
 * 
 * @author wuzhih
 * 
 */
public class CacheService {

	private static final Logger log = LoggerFactory.getLogger(CacheService.class);

	private final Map<String, CachePoolFactory> poolFactorys = new HashMap<>();
	private final Map<String, CachePool> allPools = new HashMap<>();

	public CacheService() {
		// load cache pool defined
		try {
			init();
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}
		}

	}
	public Map<String, CachePool> getAllCachePools()
	{
		return this.allPools;
	}

	private void init() throws Exception {
		Properties props = new Properties();
		InputStream in = SystemConfig.getConfigFileStream("cacheservice.properties");
		try {
			props.load(in);
		} finally {
			IoUtil.close(in);
		}

		final String poolFactoryPref = "factory.";
		final String poolKeyPref = "pool.";
		final String layedPoolKeyPref = "layedpool.";
		String[] keys = props.keySet().toArray(new String[0]);
		Arrays.sort(keys);
		for (String key : keys) {
			if (key.startsWith(poolFactoryPref)) {
				createPoolFactory(key.substring(poolFactoryPref.length()), (String) props.get(key));
			} else if (key.startsWith(poolKeyPref)) {
				String cacheName = key.substring(poolKeyPref.length());
				String value = (String) props.get(key);
				String[] valueItems = value.split(",");
				if (valueItems.length < 3) {
					String s = "Invalid cache config, key:" + key + " value:" + value;
					throw new IllegalArgumentException(s);
				}
				String type = valueItems[0];
				int size = Integer.parseInt(valueItems[1]);
				int timeOut = Integer.parseInt(valueItems[2]);
				createPool(cacheName, type, size, timeOut);
			} else if (key.startsWith(layedPoolKeyPref)) {
				String cacheName = key.substring(layedPoolKeyPref.length());
				String value = (String) props.get(key);
				String[] valueItems = value.split(",");
				int index = cacheName.indexOf(".");
				if (index < 0) {// root layer
					String type = valueItems[0];
					int size = Integer.parseInt(valueItems[1]);
					int timeOut = Integer.parseInt(valueItems[2]);
					createLayeredPool(cacheName, type, size, timeOut);
				} else {
					// root layers' children
					String parent = cacheName.substring(0, index);
					String child = cacheName.substring(index + 1);
					LayeredCachePool pool = (LayeredCachePool)this.allPools.get(parent);
					int size = Integer.parseInt(valueItems[0]);
					int timeOut = Integer.parseInt(valueItems[1]);
					pool.createChildIfAbsent(child, size, timeOut);
				}
			}
		}
	}

	private void createLayeredPool(String cacheName, String type, int size, int expireSeconds) {
		checkExists(cacheName);
		log.info("Create layered cache pool '{}' of type '{}', default cache size {}, " +
				"default expire seconds {}", cacheName, type, size, expireSeconds);
		DefaultLayeredCachePool layeredPool = new DefaultLayeredCachePool(cacheName,
			this.getCacheFact(type), size, expireSeconds);
		this.allPools.put(cacheName, layeredPool);
	}

	private void checkExists(String poolName) {
		if (allPools.containsKey(poolName)) {
			throw new IllegalArgumentException("duplicate cache pool name: " + poolName);
		}
	}

	private void createPoolFactory(String factoryType, String factoryClassName)
			throws Exception {
		CachePoolFactory factory;
		if ("mapdb".equalsIgnoreCase(factoryClassName)) {
			factory = new MapDBCachePooFactory();
		} else {
			factory = (CachePoolFactory) Class.forName(factoryClassName).newInstance();
		}
		poolFactorys.put(factoryType, factory);
	}

	private void createPool(String poolName, String type, int cacheSize, int expireSeconds) {
		checkExists(poolName);
		CachePoolFactory cacheFact = getCacheFact(type);
		CachePool cachePool = cacheFact.createCachePool(poolName, cacheSize, expireSeconds);
		this.allPools.put(poolName, cachePool);
	}

	private CachePoolFactory getCacheFact(String type) {
		CachePoolFactory facty = this.poolFactorys.get(type);
		if (facty == null) {
			throw new RuntimeException("CachePoolFactory not defined for type: " + type);
		}

		return facty;
	}

	/**
	 * get cache pool by name ,caller should cache result
	 * 
	 * @param poolName
	 * @return CachePool
	 */
	public CachePool getCachePool(String poolName) {
		CachePool pool = allPools.get(poolName);
		if (pool == null) {
			throw new IllegalArgumentException("can't find cache pool:"
					+ poolName);
		} else {
			return pool;
		}

	}

	public void clearCache() {
		log.info("clear all cache pool");
		for (CachePool pool: this.allPools.values()) {
			pool.clearCache();
		}
	}

}