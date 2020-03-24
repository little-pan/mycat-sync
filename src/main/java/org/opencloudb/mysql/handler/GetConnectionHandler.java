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
package org.opencloudb.mysql.handler;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.opencloudb.backend.BackendConnection;
import org.slf4j.*;

/**
 * wuzh
 * 
 * @author mycat
 * 
 */
public class GetConnectionHandler implements ResponseHandler {

	private static final Logger log = LoggerFactory.getLogger(GetConnectionHandler.class);

	private final AtomicInteger successCons;
	private final CountDownLatch finishLatch;
	private final int total;

	public GetConnectionHandler(int total) {
		this.successCons = new AtomicInteger();
		this.total = total;
		this.finishLatch = new CountDownLatch(total);
	}

	public String getStatusInfo() {
		long finished = this.total - this.finishLatch.getCount();
		return "finished "+finished+" success "+this.successCons+" target count:"+this.total;
	}

	public int getSuccessCons() {
		return this.successCons.get();
	}

	public boolean finished() {
		return (this.finishLatch.getCount() == 0);
	}

	public boolean await() {
		try {
			this.finishLatch.await();
			return true;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	public boolean await(long timeout, TimeUnit unit) {
		try {
			return this.finishLatch.await(timeout, unit);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		this.successCons.incrementAndGet();
		this.finishLatch.countDown();
		log.debug("Connection ok: {}", conn);
        conn.release();
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		this.finishLatch.countDown();
		if (log.isWarnEnabled()) {
            log.warn("Connection error in conn " + conn, e);
        }
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
	    if (log.isWarnEnabled()) {
            log.warn("Caught error {} in conn {}", new String(err), conn);
        }
        conn.release();
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
	    if (log.isDebugEnabled()) {
            log.debug("Received ok {} in conn {}", new String(ok), conn);
        }
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {

	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {

	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {

	}

}