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
package org.opencloudb.cache.impl;

import org.mapdb.HTreeMap;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CacheStatic;

public class MapDBCachePool implements CachePool {

	private final HTreeMap<Object, Object> hTreeMap;
	private final CacheStatic cacheStatic = new CacheStatic();
    private final long maxSize;

	public MapDBCachePool(HTreeMap<Object, Object> hTreeMap, long maxSize) {
		this.hTreeMap = hTreeMap;
		this.maxSize = maxSize;
		this.cacheStatic.setMaxSize(maxSize);
	}

	@Override
	public void putIfAbsent(Object key, Object value) {
		if (this.hTreeMap.putIfAbsent(key, value) == null) {
			this.cacheStatic.incPutTimes();
		}
	}

	@Override
	public Object get(Object key) {
		Object value = this.hTreeMap.get(key);

		if (value != null) {
			this.cacheStatic.incHitTimes();
			return value;
		} else {
			this.cacheStatic.incAccessTimes();
			return null;
		}
	}

	@Override
	public void clearCache() {
		this.hTreeMap.clear();
		this.cacheStatic.reset();
	}

	@Override
	public CacheStatic getCacheStatic() {
		this.cacheStatic.setItemSize(this.hTreeMap.sizeLong());
		return this.cacheStatic;
	}

	@Override
	public long getMaxSize() {
		return this.maxSize;
	}

}