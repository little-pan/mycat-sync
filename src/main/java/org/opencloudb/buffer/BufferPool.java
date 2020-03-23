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
package org.opencloudb.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.*;

/**
 * @author mycat
 */
public final class BufferPool {

	private static final Logger log = LoggerFactory.getLogger(BufferPool.class);
    static final boolean DIRECT = Boolean.getBoolean("org.opencloudb.buffer.direct");

	// This value not changed ,isLocalCacheThread use it
	public static final String LOCAL_BUF_THREAD_PREX = "$_";
	private  final ThreadLocalBufferPool localBufferPool;

	private final int chunkSize;
	private final ConcurrentLinkedQueue<ByteBuffer> items = new ConcurrentLinkedQueue<ByteBuffer>();
	private long sharedOptsCount;
	//private volatile int newCreated;
    private AtomicInteger newCreated = new AtomicInteger();
	private final long threadLocalCount;
	private final long capactiy;
	private long totalBytes = 0;
	private long totalCounts = 0;

	public BufferPool(long bufferSize, int chunkSize, int threadLocalPercent) {
		this.chunkSize = chunkSize;
		long size = bufferSize / chunkSize;
		size = (bufferSize % chunkSize == 0) ? size : size + 1;
		this.capactiy = size;
		this.threadLocalCount = threadLocalPercent * this.capactiy / 100;
		this.localBufferPool = new ThreadLocalBufferPool(this.threadLocalCount);
	}

	private static final boolean isLocalCacheThread() {
		final String threadName = Thread.currentThread().getName();
		return (threadName.startsWith(LOCAL_BUF_THREAD_PREX));
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public long getSharedOptsCount() {
		return sharedOptsCount;
	}

	public long size() {
		return this.items.size();
	}

	public long capacity() {
		return capactiy + newCreated.get();
	}

	public ByteBuffer allocate() {
		ByteBuffer node = null;
		if (isLocalCacheThread()) {
			// allocate from threadlocal
			node = this.localBufferPool.get().poll();
			if (node != null) {
				return node;
			}
		}
		node = this.items.poll();
		if (node == null) {
			//newCreated++;
			final int size = this.chunkSize;
			node = DIRECT? createDirectBuffer(size): createHeapBuffer(size);
            this.newCreated.incrementAndGet();
		}

		return node;
	}

	private boolean checkValidBuffer(ByteBuffer buffer) {
		// 拒绝回收null和容量大于chunkSize的缓存
		if (buffer == null || DIRECT != buffer.isDirect()) {
			return false;
		} else if (buffer.capacity() > this.chunkSize) {
			log.warn("Cant' recycle a buffer larger than pool chunkSize {}", buffer.capacity());
			return false;
		}
        this.totalCounts++;
        this.totalBytes += buffer.limit();
		buffer.clear();

		return true;
	}

	public void recycle(ByteBuffer buffer) {
		if (!checkValidBuffer(buffer)) {
			return;
		}

		if (isLocalCacheThread()) {
			BufferQueue localQueue = this.localBufferPool.get();
			if (localQueue.snapshotSize() < this.threadLocalCount) {
				localQueue.put(buffer);
			} else {
				// recyle 3/4 thread local buffer
                this.items.addAll(localQueue.removeItems(this.threadLocalCount * 3 / 4));
                this.items.offer(buffer);
                this.sharedOptsCount++;
			}
		} else {
            this.sharedOptsCount++;
            this.items.offer(buffer);
		}
	}

	public int getAvgBufSize() {
		if (this.totalBytes < 0) {
			totalBytes = 0;
			this.totalCounts = 0;
			return 0;
		} else {
			return (int) (totalBytes / totalCounts);
		}
	}

	public boolean testIfDuplicate(ByteBuffer buffer) {
		for (ByteBuffer exists : items) {
			if (exists == buffer) {
				return true;
			}
		}
		return false;
	}

	private ByteBuffer createHeapBuffer(int size) {
		return ByteBuffer.allocate(size);
	}

	private ByteBuffer createDirectBuffer(int size) {
		// For performance
		return ByteBuffer.allocateDirect(size);
	}

	public ByteBuffer allocate(int size) {
		if (size <= this.chunkSize) {
			return allocate();
		} else {
			log.warn("Allocate buffer size {}(larger than pool chunkSize {})", size, this.chunkSize);
			return createHeapBuffer(size);
		}
	}

	public static void main(String[] args) {
		BufferPool pool = new BufferPool(3276800000L, 1024, 2);
		long i = pool.capacity();
		List<ByteBuffer> all = new ArrayList<ByteBuffer>();
		for (int j = 0; j <= i; j++) {
			all.add(pool.allocate());
		}
		for (ByteBuffer buf : all) {
			pool.recycle(buf);
		}
		log.info("pool size {}", pool.size());
	}

}
