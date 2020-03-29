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

import org.opencloudb.net.BackendConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.ServerSession;
import org.slf4j.*;

import java.util.Iterator;
import java.util.Map;

/**
 * @author mycat
 */
public class RollbackNodeHandler extends MultiNodeHandler {

	static final Logger log = LoggerFactory.getLogger(RollbackNodeHandler.class);

	public RollbackNodeHandler(ServerSession session) {
		super(session);
	}

	public void rollback() {
		log.debug("rollback in session {}", this.session);

		Map<RouteResultsetNode, BackendConnection> targets = this.session.getTargetMap();
		Iterator<Map.Entry<RouteResultsetNode, BackendConnection>> it = targets.entrySet().iterator();
		int initCount = this.session.getTargetCount();

		reset(initCount);
		for (; it.hasNext(); ) {
			Map.Entry<RouteResultsetNode, BackendConnection> target = it.next();
			BackendConnection conn = target.getValue();
			log.debug("'rollback' in backend {}", conn);
			conn.setResponseHandler(this);
			conn.rollback();
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		log.debug("'rollback' ok in backend {}", conn);

		if (decrementCountBy(1)) {
			if (isFail() || this.session.closed()) {
				tryErrorFinished(true);
			} else {
				log.debug("'rollback' all ok in session {}", this.session);
				// Clear all resources
				this.session.clearResources();
				ServerConnection source = this.session.getSource();
				source.write(ok);
			}
		}
	}

	@Override
	protected void tryErrorFinished(boolean allEnd) {
		if (allEnd && !this.session.closed() && !this.errorResponsed) {
			// Note: "rollback" always OK even if error occurs, otherwise can't do anything.
			// If error happened, it's safe when we close all backend connections.
			ServerConnection source = this.session.getSource();
			source.write(OkPacket.OK);
			// Clear session resources and release all
			log.debug("Error finished and cleanup resources in session {}", this.session);
			this.session.closeAndClearResources(this.error);
			this.errorResponsed = true;
		}
	}

}