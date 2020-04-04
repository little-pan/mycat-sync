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
package org.opencloudb.mysql.nio;

import java.util.ArrayList;
import java.util.List;

import org.opencloudb.mysql.ByteUtil;
import org.opencloudb.mysql.handler.LoadDataResponseHandler;
import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.net.handler.BackendAsyncHandler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RequestFilePacket;
import org.slf4j.*;

/**
 * life cycle: from connection establish to close <br/>
 * 
 * @author mycat
 */
public class MySQLConnectionHandler extends BackendAsyncHandler {

	static final Logger log = LoggerFactory.getLogger(MySQLConnectionHandler.class);

	private static final int RESULT_STATUS_INIT = 0;
	private static final int RESULT_STATUS_HEADER = 1;
	private static final int RESULT_STATUS_FIELD_EOF = 2;

	private final MySQLConnection source;
	private int resultStatus;
	private byte[] header;
	private List<byte[]> fields;

	/**
	 * life cycle: one SQL execution
	 */
	private ResponseHandler responseHandler;

	public MySQLConnectionHandler(MySQLConnection source) {
		this.source = source;
		this.resultStatus = RESULT_STATUS_INIT;
	}

	public void connectionError(Throwable e) {
		this.responseHandler.connectionError(e, this.source);
	}

	public MySQLConnection getSource() {
		return this.source;
	}

	@Override
	public void handle(byte[] data) {
		offerData(data);
	}

	@Override
	protected void offerDataError() {
		this.resultStatus = RESULT_STATUS_INIT;
		throw new RuntimeException("Offer data error!");
	}

	@Override
	protected void handleData(byte[] data) {
		switch (this.resultStatus) {
		case RESULT_STATUS_INIT:
			switch (data[4]) {
			case OkPacket.FIELD_COUNT:
				handleOkPacket(data);
				break;
			case ErrorPacket.FIELD_COUNT:
				handleErrorPacket(data);
				break;
			case RequestFilePacket.FIELD_COUNT:
				handleRequestPacket(data);
				break;
			default:
				this.resultStatus = RESULT_STATUS_HEADER;
				this.header = data;
				int fieldCount = (int) ByteUtil.readLength(data, 4);
				this.fields = new ArrayList<>(fieldCount);
				break;
			}
			break;
		case RESULT_STATUS_HEADER:
			switch (data[4]) {
			case ErrorPacket.FIELD_COUNT:
				this.resultStatus = RESULT_STATUS_INIT;
				handleErrorPacket(data);
				break;
			case EOFPacket.FIELD_COUNT:
				this.resultStatus = RESULT_STATUS_FIELD_EOF;
				handleFieldEofPacket(data);
				break;
			default:
				this.fields.add(data);
				break;
			}
			break;
		case RESULT_STATUS_FIELD_EOF:
			switch (data[4]) {
			case ErrorPacket.FIELD_COUNT:
				this.resultStatus = RESULT_STATUS_INIT;
				handleErrorPacket(data);
				break;
			case EOFPacket.FIELD_COUNT:
				this.resultStatus = RESULT_STATUS_INIT;
				handleRowEofPacket(data);
				break;
			default:
				handleRowPacket(data);
				break;
			}
			break;
		default:
			throw new IllegalStateException("Unknown status!");
		}
	}

	public void setResponseHandler(ResponseHandler responseHandler) {
		this.responseHandler = responseHandler;
	}

	/**
	 * OK数据包处理
	 */
	private void handleOkPacket(byte[] data) {
		ResponseHandler respHand = this.responseHandler;
		if (respHand != null) {
			respHand.okResponse(data, this.source);
		}
	}

	/**
	 * ERROR数据包处理
	 */
	private void handleErrorPacket(byte[] data) {
		ResponseHandler respHand = this.responseHandler;
		if (respHand != null) {
			respHand.errorResponse(data, this.source);
		} else {
			closeNoHandler();
		}
	}

	/**
	 * load data file 请求文件数据包处理
	 */
	private void handleRequestPacket(byte[] data) {
		ResponseHandler respHand = this.responseHandler;
		if (respHand instanceof LoadDataResponseHandler) {
			((LoadDataResponseHandler) respHand).requestDataResponse(data, this.source);
		} else {
			closeNoHandler();
		}
	}

	/**
	 * 字段数据包结束处理
	 */
	private void handleFieldEofPacket(byte[] data) {
		ResponseHandler respHand = this.responseHandler;
		if (respHand != null) {
			respHand.fieldEofResponse(this.header, this.fields, data, this.source);
			this.fields = null;
		} else {
			closeNoHandler();
		}
	}

	/**
	 * 行数据包处理
	 */
	private void handleRowPacket(byte[] data) {
		ResponseHandler respHand = this.responseHandler;
		if (respHand != null) {
			respHand.rowResponse(data, this.source);
		} else {
			closeNoHandler();
		}
	}

	private void closeNoHandler() {
		if (!this.source.isClosedOrQuit()) {
			this.source.close("no handler");
			log.warn("no handler bind in this con {},  client {}", this, this.source);
		}
	}

	/**
	 * 行数据包结束处理
	 */
	private void handleRowEofPacket(byte[] data) {
		if (this.responseHandler != null) {
			this.responseHandler.rowEofResponse(data, this.source);
		} else {
			closeNoHandler();
		}
	}

}