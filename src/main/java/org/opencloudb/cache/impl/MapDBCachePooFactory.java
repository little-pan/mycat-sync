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

import java.util.concurrent.TimeUnit;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CachePoolFactory;

import static java.lang.Integer.*;
import static java.lang.Double.*;
import static java.lang.System.*;

public class MapDBCachePooFactory extends CachePoolFactory {

	static final String PROP_PREFIX = "org.opencloudb.cache.mapdb.";

	static final int CACHE_SIZE = getInteger(PROP_PREFIX+"cacheSize", 102400);
	// Default 64MB
	static final double SIZE_LIMIT = parseDouble(getProperty(PROP_PREFIX+"sizeLimit", "0.064"));

	private final DB db = DBMaker.newTempFileDB()
			.asyncWriteEnable()
			.transactionDisable()
			.commitFileSyncDisable()
			.cacheSize(CACHE_SIZE)
			.sizeLimit(SIZE_LIMIT)
			.cacheLRUEnable()
			.make();

	@Override
	public CachePool createCachePool(String poolName, int cacheSize, int expiredSeconds) {
		HTreeMap<Object, Object> cache = this.db
				.createHashMap(poolName)
				.expireMaxSize(cacheSize)
				.expireAfterAccess(expiredSeconds, TimeUnit.SECONDS)
				.makeOrGet();
		return new MapDBCachePool(cache, cacheSize);
	}

}