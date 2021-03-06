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
package org.opencloudb.backend;

import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.route.RouteResultsetNode;

import java.io.IOException;

public class PhysicalDBNode {

	protected final String name;
	protected final String database;
	protected final PhysicalDBPool dbPool;

	public PhysicalDBNode(String hostName, String database, PhysicalDBPool dbPool) {
		this.name = hostName;
		this.database = database;
		this.dbPool = dbPool;
	}

	public String getName() {
		return name;
	}

	public PhysicalDBPool getDbPool() {
		return dbPool;
	}

	public String getDatabase() {
		return database;
	}

	private void checkRequest(String schema){
		if (schema != null && !schema.equals(this.database)) {
			throw new RuntimeException("invalid param, connection request db is :"
								+ schema + " and datanode db is " + this.database);
		}
		if (!this.dbPool.isInitSuccess()) {
			this.dbPool.init(this.dbPool.activedIndex);
		}
	}

	public void getConnection(String schema, boolean autoCommit, RouteResultsetNode rrs,
							  ResponseHandler handler, Object attachment) {
		checkRequest(schema);
		if (this.dbPool.isInitSuccess()) {
			if (rrs.canRunINReadDB(autoCommit)) {
				this.dbPool.getRWBalanceCon(schema, autoCommit, rrs, handler, attachment, this.database);
			} else {
				this.dbPool.getSource().getConnection(schema, autoCommit, rrs, handler, attachment);
			}
		} else {
			String errmsg = String.format("Uninitialized dataHost '%s': active node host index %s",
					this.dbPool.getHostName(), this.dbPool.getActivedIndex());
			handler.connectionError(new IOException(errmsg), null);
		}
	}

}
