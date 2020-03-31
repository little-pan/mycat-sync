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

import java.util.HashMap;
import java.util.Map;

import org.opencloudb.cache.CacheStatic;
import org.opencloudb.cache.LayeredCachePool;

public class SimpleCachePool implements LayeredCachePool {

	private HashMap<Object, Object> cacheMap = new HashMap<>();
	private Map<String, LayeredCachePool> children = new HashMap<>();

	@Override
	public void putIfAbsent(Object key, Object value) {
		this.cacheMap.put(key, value);
	}

	@Override
	public Object get(Object key) {
		return this.cacheMap.get(key);
	}

	@Override
	public void clearCache() {
		this.cacheMap.clear();
	}

	@Override
	public CacheStatic getCacheStatic() {
		return null;
	}

	@Override
	public void putIfAbsent(String primaryKey, Object secondKey, Object value) {
		String ck = primaryKey+"_"+secondKey;
		LayeredCachePool child = this.children.get(ck);
		if (child == null) {
			child = createChildIfAbsent(ck, Integer.MAX_VALUE, -1);
		}
		child.putIfAbsent(secondKey, value);
	}

	@Override
	public LayeredCachePool createChildIfAbsent(String cacheName, int cacheSize, int expiredSeconds) {
		LayeredCachePool child = new SimpleCachePool();
		this.children.put(cacheName, child);
		return child;
	}

	@Override
	public Object get(String primaryKey, Object secondKey) {
		return get(primaryKey+"_"+secondKey);
	}

	@Override
	public Map<String, CacheStatic> getAllCacheStatic() {
		return null;
	}

	@Override
	public long getMaxSize() {
		return 100;
	}

}
