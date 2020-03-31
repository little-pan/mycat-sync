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

/**
 * Test cache performance, for mapdb set vm param -server -Xms100M -Xmx100M
 * Test result eg. write 40k+/s, read 60k+/s.
 */
import org.opencloudb.cache.impl.MapDBCachePooFactory;

public class TestCachePoolPerformance {

	private CachePool pool;
	private int maxCacheCount = 100 * 10000;

	public static CachePool createMapDBCachePool() {
		MapDBCachePooFactory fact = new MapDBCachePooFactory();
		return fact.createCachePool("mapdbcache", 100 * 10000, 3600);
	}

	public void test() {
		testSwarm();
		testInsertSpeed();
		testSelectSpeed();
	}

	private void testSwarm() {
		System.out.println("prepare ........");
		for (int i = 0; i < 100000; i++) {
			pool.putIfAbsent(i % 100, "dn1");
		}
		for (int i = 0; i < 100000; i++) {
			pool.get(i % 100);
		}
		pool.clearCache();
	}

	private void testSelectSpeed() {
		System.out.println("test select speed for " + this.pool + " count:"
				+ this.maxCacheCount);
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < maxCacheCount; i++) {
			pool.get(i + "");
		}
		double used = (System.currentTimeMillis() - startTime) / 1000.0;
		CacheStatic statics = pool.getCacheStatic();
		System.out.println("used time:" + used + " tps:" + maxCacheCount / used
				+ " cache hit:" + 100 * statics.getHitTimes()
				/ statics.getAccessTimes());
	}

	private void GC() {
		for (int i = 0; i < 5; i++) {
			System.gc();
		}
	}

	private void testInsertSpeed() {
		this.GC();
		long freeMem = Runtime.getRuntime().freeMemory();
		System.out.println("test insert speed for " + this.pool
				+ " with insert count:" + this.maxCacheCount);
		long start = System.currentTimeMillis();
		for (int i = 0; i < maxCacheCount; i++) {
			try {
				pool.putIfAbsent(i + "", "dn" + i % 100);
			} catch (Error e) {
				System.out.println("insert " + i + " error");
				e.printStackTrace();
				break;
			}
		}
		long used = (System.currentTimeMillis() - start) / 1000;
		long count = pool.getCacheStatic().getItemSize();
		this.GC();
		long usedMem = freeMem - Runtime.getRuntime().freeMemory();
		System.out.println(" cache size is " + count + " ,all in cache :"
				+ (count == maxCacheCount) + " ,used time:" + used + " ,tps:"
				+ count / used + " used memory:" + usedMem / 1024 / 1024 + "M");
	}

	public static void main(String[] args) {
		TestCachePoolPerformance tester = new TestCachePoolPerformance();
		tester.pool = createMapDBCachePool();
		tester.test();
	}

}
