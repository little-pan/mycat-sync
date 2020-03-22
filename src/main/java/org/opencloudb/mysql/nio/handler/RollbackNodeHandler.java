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
package org.opencloudb.mysql.nio.handler;

import java.util.List;

import org.opencloudb.backend.BackendConnection;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.slf4j.*;

/**
 * @author mycat
 */
public class RollbackNodeHandler extends MultiNodeHandler {

	private static final Logger log = LoggerFactory.getLogger(RollbackNodeHandler.class);

	public RollbackNodeHandler(NonBlockingSession session) {
		super(session);
	}

	public void rollback() {
		final int initCount = this.session.getTargetCount();
		this.lock.lock();
		try {
			reset(initCount);
		} finally {
			this.lock.unlock();
		}
		if (this.session.closed()) {
			decrementCountToZero();
			return;
		}

		// 执行
		int started = 0;
		for (final RouteResultsetNode node : this.session.getTargetKeys()) {
			if (node == null) {
                log.error("null is contained in RouteResultsetNodes, source = {}", this.session.getSource());
				continue;
			}
			final BackendConnection conn =  this.session.getTarget(node);
			if (conn != null) {
                log.debug("rollback job run for backend {}", conn);
				if (clearIfSessionClosed(this.session)) {
					return;
				}
				conn.setResponseHandler(RollbackNodeHandler.this);
				conn.rollback();
				++started;
			}
		}

		if (started < initCount && decrementCountBy(initCount - started)) {
			/**
			 * assumption: only caused by front-end connection close. <br/>
			 * Otherwise, packet must be returned to front-end
			 */
			this.session.clearResources(true);
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (decrementCountBy(1)) {
			// clear all resources
			session.clearResources(false);
			if (this.isFail() || session.closed()) {
				tryErrorFinished(true);
			} else {
				session.getSource().write(ok);
			}
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		log.error("Unexpected packet for backend {} bound by source {}", conn, this.session.getSource());
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
        log.error("Unexpected packet for backend {} bound by source {}", conn, this.session.getSource());
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
        log.error("Unexpected packet for backend {} bound by source {}", conn, this.session.getSource());
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
        log.error("Unexpected packet for backend {} bound by source {}", conn, this.session.getSource());
	}

	@Override
	public void writeQueueAvailable() {

	}

}