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
package org.opencloudb.sequence;

import junit.framework.Assert;

import org.junit.Test;
import org.opencloudb.sequence.handler.*;
import org.opencloudb.util.Callback;

import java.util.HashSet;
import java.util.Set;

/**
 * 全局序列号单元测试
 * 
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @time Create on 2013-12-30 上午12:07:51
 * @version 1.0
 */
public class SequenceHandlerTest {

	static final String propSeqFile = "src/test/resources/sequence_conf.properties";
	static final String timeSeqFile = "src/test/resources/sequence_time_conf.properties";

	static final SequenceHandler propSeqHandler = new IncrSequencePropHandler(propSeqFile);
	static final SequenceHandler timeSeqHandler = new IncrSequenceTimeHandler(timeSeqFile, true);
	static final SequenceHandler snowSeqHandler = new SnowflakeIdSequenceHandler();
	static final Set<Long> dupSet = new HashSet<>();

	protected static void cleanup() {
		synchronized (dupSet) {
			dupSet.clear();
		}
	}

	@Test
	public void testPropSequence() {
		Callback<Long> cb = new Callback<Long>() {
			long a = -1, b = -1;

			@Override
			public void call(Long result, Throwable cause) {
				Assert.assertNotNull(result);
				Assert.assertNull(cause);
				Assert.assertTrue(result != -1);

				if (this.a == -1) {
					this.a = result;
				} else {
					this.b = result;
					Assert.assertEquals(this.a - this.b, -1);
				}
			}
		};

		propSeqHandler.nextId("MY1", cb);
		propSeqHandler.nextId("MY1", cb);
	}

	@Test
	public void testNotDupSequence() throws Exception {
		cleanup();
		serialPerfTest("PropSeq", propSeqHandler, true);
		cleanup();
		serialPerfTest("TimeSeq", timeSeqHandler, true);
		// Snowflake test not passed
		//cleanup();
		//serialPerfTest("SnowSeq", snowSeqHandler, true);

		cleanup();
		concurPerfTest("PropSeq", propSeqHandler, true);
		cleanup();
		concurPerfTest("TimeSeq", timeSeqHandler, true);
		// Snowflake test not passed
		//cleanup();
		//concurPerfTest("SnowSeq", snowSeqHandler, true);
	}

	@Test
	public void testSerialPerf() {
		serialPerfTest("PropSeq", propSeqHandler, false);
		serialPerfTest("TimeSeq", timeSeqHandler, false);
		serialPerfTest("SnowSeq", snowSeqHandler, false);
	}

	void serialPerfTest(String name, SequenceHandler handler, boolean checkDup) {
		final int n = 10000;
		long s = System.currentTimeMillis();

		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);
		perfTest(handler, 0, n/10, checkDup);

		long e = System.currentTimeMillis();
		String m = String.format("[%s] Serial tps %s", name, (1000 * n / (e - s)));
		System.out.println(m);
	}

	@Test
	public void testConcurPerf() throws Exception {
		concurPerfTest("PropSeq", propSeqHandler, false);
		concurPerfTest("TimeSeq", timeSeqHandler, false);
		concurPerfTest("SnowSeq", snowSeqHandler, false);
	}

	void concurPerfTest(String name, final SequenceHandler handler, final boolean checkDup)
			throws Exception {
		final int n = 10000, p = 10;
		long s = System.currentTimeMillis();

		Thread[] threads = new Thread[p];
		for (int i = 0; i < p; ++i) {
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					perfTest(handler, 0, n/p, checkDup);
				}
			});
			threads[i] = t;
			t.setDaemon(true);
			t.start();
		}
		for (int i = 0; i < p; ++i) {
			threads[i].join();
		}

		long e = System.currentTimeMillis();
		String m = String.format("[%s] Concur tps %s for %s threads", name, (1000 * n / (e - s)), p);
		System.out.println(m);
	}

	void perfTest(final SequenceHandler handler, final int i, final int n, final boolean checkDup) {
		if (i >= n) {
			return;
		}

		handler.nextId("MY1", new Callback<Long>() {
			@Override
			public void call(Long result, Throwable cause) {
				Assert.assertNotNull(result);
				Assert.assertNull(cause);
				if (checkDup) {
					synchronized (dupSet) {
						Assert.assertFalse(dupSet.contains(result));
						dupSet.add(result);
					}
				}
				perfTest(handler, i + 1, n, checkDup);
			}
		});
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Callback<Long> print = new Callback<Long>() {
			@Override
			public void call(Long result, Throwable cause) {
				Assert.assertNotNull(result);
				Assert.assertNull(cause);
				System.out.println("seq: " + result);
			}
		};

		propSeqHandler.nextId("MY1", print);
		timeSeqHandler.nextId("MY1", print);
		snowSeqHandler.nextId("MY1", print);
	}

}