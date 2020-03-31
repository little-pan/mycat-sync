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

import org.slf4j.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultLayeredCachePool implements LayeredCachePool {

	private static final Logger log = LoggerFactory.getLogger(DefaultLayeredCachePool.class);

	protected static final String defaultCache = "default";
	public static final String DEFAULT_CACHE_COUNT = "DEFAULT_CACHE_COUNT";
	public static final String DEFAULT_CACHE_EXPIRE_SECONDS = "DEFAULT_CACHE_EXPIRE_SECONDS";

	protected Map<String, CachePool> allCaches = new HashMap<>();
	protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	protected int defaultCacheSize;
	protected int defaultExpiredSeconds;
	private final CachePoolFactory poolFactory;
	private final String name;

	public DefaultLayeredCachePool(String name, CachePoolFactory poolFactory,
			int defaultCacheSize, int defaultExpiredSeconds) {
		this.name = name;
		this.poolFactory = poolFactory;
		this.defaultCacheSize = defaultCacheSize;
		this.defaultExpiredSeconds = defaultExpiredSeconds;
	}

	private CachePool getCache(String cacheName) {
		this.lock.readLock().lock();
		try {
			final CachePool pool = this.allCaches.get(cacheName);
			if (pool != null) {
				return pool;
			}
		} finally {
			this.lock.readLock().unlock();
		}

		return createChildIfAbsent(cacheName, this.defaultCacheSize, this.defaultExpiredSeconds);
	}

	@Override
	public CachePool createChildIfAbsent(String cacheName, int cacheSize, int expiredSeconds) {
		this.lock.writeLock().lock();
		try {
			CachePool pool = this.allCaches.get(cacheName);
			if (pool != null) {
				return pool;
			}

			log.info("Create child Cache '{}' for layered cache '{}', size {}, expire seconds {}",
													cacheName, this.name, cacheSize, expiredSeconds);
			String childName = this.name + "." + cacheName;
			pool = this.poolFactory.createCachePool(childName, cacheSize, expiredSeconds);
			this.allCaches.put(cacheName, pool);

			return pool;
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public void putIfAbsent(Object key, Object value) {
		putIfAbsent(defaultCache, key, value);
	}

	@Override
	public Object get(Object key) {
		return get(defaultCache, key);
	}

	@Override
	public void clearCache() {
		log.info("clear cache '{}'", this.name);
		this.lock.writeLock().lock();
		try {
			for (CachePool pool : this.allCaches.values()) {
				pool.clearCache();
			}
		} finally {
			this.lock.writeLock().unlock();
		}
	}

	@Override
	public void putIfAbsent(String primaryKey, Object secondKey, Object value) {
		CachePool pool = getCache(primaryKey);
		pool.putIfAbsent(secondKey, value);
	}

	@Override
	public Object get(String primaryKey, Object secondKey) {
		CachePool pool = getCache(primaryKey);
		return pool.get(secondKey);
	}

	@Override
	public CacheStatic getCacheStatic() {
		CacheStatic cacheStatic = new CacheStatic();
		cacheStatic.setMaxSize(this.getMaxSize());
		for (CacheStatic singleStatic : getAllCacheStatic().values()) {
			cacheStatic.setItemSize(cacheStatic.getItemSize() + singleStatic.getItemSize());
			cacheStatic.setHitTimes(cacheStatic.getHitTimes() + singleStatic.getHitTimes());
			cacheStatic.setAccessTimes(cacheStatic.getAccessTimes() + singleStatic.getAccessTimes());
			cacheStatic.setPutTimes(cacheStatic.getPutTimes() + singleStatic.getPutTimes());
			if (cacheStatic.getLastAccesTime() < singleStatic.getLastAccesTime()) {
				cacheStatic.setLastAccesTime(singleStatic.getLastAccesTime());
			}
			if (cacheStatic.getLastPutTime() < singleStatic.getLastPutTime()) {
				cacheStatic.setLastPutTime(singleStatic.getLastPutTime());
			}
		}

		return cacheStatic;
	}

	@Override
	public Map<String, CacheStatic> getAllCacheStatic() {
		this.lock.readLock().lock();
		try {
			Map<String, CacheStatic> results = new HashMap<>(this.allCaches.size());
			for (Map.Entry<String, CachePool> entry : this.allCaches.entrySet()) {
				results.put(entry.getKey(), entry.getValue().getCacheStatic());
			}
			return results;
		} finally {
			this.lock.readLock().unlock();
		}
	}

	@Override
	public long getMaxSize() {
		long maxSize = 0;
		this.lock.readLock().lock();
		try {
			for(CachePool cache: this.allCaches.values()) {
				maxSize += cache.getMaxSize();
			}
			return maxSize;
		} finally {
			this.lock.readLock().unlock();
		}
	}

}
