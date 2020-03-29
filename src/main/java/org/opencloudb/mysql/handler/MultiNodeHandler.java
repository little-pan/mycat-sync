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

import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.ServerSession;
import org.opencloudb.util.StringUtil;
import org.slf4j.*;

/**
 * @author mycat
 */
abstract class MultiNodeHandler extends AbstractResponseHandler implements Terminatable {

	private static final Logger log = LoggerFactory.getLogger(MultiNodeHandler.class);

	protected final ServerSession session;
	private boolean isFailed = false;
	protected String error;
	protected boolean errorResponsed = false;
	protected boolean isClosedByDiscard = false;
	protected byte packetId;

	private int nodeCount;
	private Runnable terminateCallBack;

	public MultiNodeHandler(ServerSession session) {
		if (session == null) {
			throw new IllegalArgumentException("session is null!");
		}
		this.session = session;
	}

	public void setFail(String errMsg) {
		this.isFailed = true;
		this.error = errMsg;
	}

	public boolean isFail() {
		return this.isFailed;
	}

	@Override
	public void terminate(Runnable terminateCallBack) {
		boolean zeroReached = false;
		if (this.nodeCount > 0) {
			this.terminateCallBack = terminateCallBack;
		} else {
			zeroReached = true;
		}
		if (zeroReached) {
			terminateCallBack.run();
		}
	}

	protected boolean canClose(BackendConnection conn, boolean failed) {
		// release this connection if safe
		this.session.releaseConnectionIfSafe(conn);
		boolean allFinished = false;
		if (failed) {
			allFinished = decrementCountBy(1);
			tryErrorFinished(allFinished);
		}

		return allFinished;
	}

	protected void decrementCountToZero() {
		Runnable callback;
		this.nodeCount = 0;
		callback = this.terminateCallBack;
		this.terminateCallBack = null;
		if (callback != null) {
			callback.run();
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		setFail("Connection failed: " + e);
		boolean allEnd = decrementCountBy(1);
		tryErrorFinished(allEnd);
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		this.session.releaseConnectionIfSafe(conn);

		ErrorPacket err = new ErrorPacket();
		err.read(data);
		String errmsg = new String(err.message);
		setFail(errmsg);
		log.warn("Error response: errmsg '{}',  errno {} in backend {}" , errmsg, err.errno, conn);

		boolean allEnd = decrementCountBy(1);
		tryErrorFinished(allEnd);
	}

	public boolean clearIfSessionClosed(ServerSession session) {
		if (session.closed()) {
			log.debug("session closed, clear resources in session {} ", session);
			session.clearResources();
			clearResources();
			return true;
		} else {
			return false;
		}
	}

	protected boolean decrementCountBy(final int count) {
		final boolean zeroReached;
		Runnable callback = null;

		this.nodeCount -= count;
		if (zeroReached = this.nodeCount <= 0) {
			callback = this.terminateCallBack;
			this.terminateCallBack = null;
		}
		if (zeroReached && callback != null) {
			callback.run();
		}

		return zeroReached;
	}

	protected void reset(int initCount) {
		this.nodeCount = initCount;
		this.isFailed = false;
		this.error = null;
		this.packetId = 0;
	}

	protected ErrorPacket createErrPkg(String errmgs) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++this.packetId;
		err.errno = ErrorCode.ER_UNKNOWN_ERROR;
		err.message = StringUtil.encode(errmgs, session.getSource().getCharset());
		return err;
	}

	protected void tryErrorFinished(boolean allEnd) {
		if (allEnd && !this.session.closed() && !this.errorResponsed) {
			ServerConnection source = this.session.getSource();
			createErrPkg(this.error).write(source);
			// Clear session resources, release all
			log.debug("all error finished, clear session resources in session {}", this.session);
			if (this.session.getSource().isAutocommit()) {
				this.session.closeAndClearResources(this.error);
			} else {
				this.session.getSource().setTxInterrupt(this.error);
				// clear resources
				clearResources();
			}
			this.errorResponsed = true;
		}
	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		if(this.isClosedByDiscard){
			log.debug("Close backend but 'isClosedByDiscard' is set:" +
						" close reason '{}', backend {}", reason, conn);
			return;
		}

		setFail(reason);
		boolean finished = (this.nodeCount == 0);
		if (!finished) {
			finished = decrementCountBy(1);
		}
		if (this.error == null) {
			this.error = "Backend connection closed";
		}
		tryErrorFinished(finished);
	}

	public void clearResources() {

	}

}
