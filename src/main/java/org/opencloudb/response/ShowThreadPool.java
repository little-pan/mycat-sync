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
package org.opencloudb.response;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.opencloudb.MycatServer;
import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.NioProcessorPool;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.IntegerUtil;
import org.opencloudb.util.LongUtil;
import org.opencloudb.util.NameableExecutor;
import org.opencloudb.util.StringUtil;

/**
 * 查看线程池状态
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowThreadPool {

	private static final int FIELD_COUNT = 6;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("POOL_SIZE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TASK_QUEUE_SIZE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("COMPLETED_TASK", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TOTAL_TASK", Fields.FIELD_TYPE_LONGLONG);
		fields[i].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c) {
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c, true);
		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}

		// write eof
		buffer = eof.write(buffer, c, true);

		// write rows
		byte packetId = eof.packetId;
		String charset = c.getCharset();

		for (NameableExecutor exec : getExecutors()) {
			RowDataPacket row = getRow(exec, charset);
			row.packetId = ++packetId;
			buffer = row.write(buffer, c, true);
		}

		MycatServer server = MycatServer.getContextServer();
		ThreadPoolExecutor mycatTimer = server.getMycatTimer();
		RowDataPacket row = getRow(MycatServer.NAME+"Timer", mycatTimer, charset);
		row.packetId = ++packetId;
		buffer = row.write(buffer, c, true);

		for (NioProcessorPool pool : getProcessorPools()) {
			row = getRow(pool.getName(), pool, charset);
			row.packetId = ++packetId;
			buffer = row.write(buffer, c, true);
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// write buffer
		c.write(buffer);
	}

	private static RowDataPacket getRow(NameableExecutor exec, String charset) {
		return getRow(exec.getName(), exec, charset);
	}

	private static RowDataPacket getRow(String name, ThreadPoolExecutor exec, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(name, charset));
		row.add(IntegerUtil.toBytes(exec.getPoolSize()));
		row.add(IntegerUtil.toBytes(exec.getActiveCount()));
		row.add(IntegerUtil.toBytes(exec.getQueue().size()));
		row.add(LongUtil.toBytes(exec.getCompletedTaskCount()));
		row.add(LongUtil.toBytes(exec.getTaskCount()));

		return row;
	}

	private static RowDataPacket getRow(String name, NioProcessorPool pool, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(name, charset));
		row.add(IntegerUtil.toBytes(pool.getPoolSize()));
		row.add(IntegerUtil.toBytes(pool.getActiveCount()));
		row.add(IntegerUtil.toBytes(pool.getQueueSize()));
		row.add(LongUtil.toBytes(pool.getCompletedTaskCount()));
		row.add(LongUtil.toBytes(pool.getTaskCount()));

		return row;
	}

	private static List<NameableExecutor> getExecutors() {
		MycatServer server = MycatServer.getContextServer();
		List<NameableExecutor> list = new ArrayList<>(1);

		list.add(server.getTimerExecutor());
		return list;
	}

	private static List<NioProcessorPool> getProcessorPools() {
		MycatServer server = MycatServer.getContextServer();
		List<NioProcessorPool> list = new ArrayList<>(2);

		list.add(server.getProcessorPool());
		list.add(server.getManagerProcessorPool());
		return list;
	}

}