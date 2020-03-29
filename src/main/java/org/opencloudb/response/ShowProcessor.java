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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.opencloudb.MycatServer;
import org.opencloudb.buffer.BufferPool;
import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.ConnectionManager;
import org.opencloudb.net.FrontendException;
import org.opencloudb.net.NioProcessor;
import org.opencloudb.net.NioProcessorPool;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.IntegerUtil;
import org.opencloudb.util.LongUtil;

/**
 * 查看处理器状态
 * 
 * @author mycat
 * @author mycat
 */
public final class ShowProcessor {

    private static final int FIELD_COUNT = 12;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REACT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("R_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("W_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FREE_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_BUFFER", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("BU_PERCENT", Fields.FIELD_TYPE_TINY);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BU_WARNS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("FC_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BC_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);
        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }
        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        MycatServer server = MycatServer.getContextServer();
        NioProcessorPool pool;
        byte packetId = eof.packetId;

        pool = server.getProcessorPool();
        for (NioProcessor p: pool.getProcessors()) {
            RowDataPacket row = getRow(p, c.getCharset());
            row.packetId = ++packetId;
            buffer = row.write(buffer, c,true);
        }

        pool = server.getManagerProcessorPool();
        for (NioProcessor p: pool.getProcessors()) {
            RowDataPacket row = getRow(p, c.getCharset());
            row.packetId = ++packetId;
            buffer = row.write(buffer, c,true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(NioProcessor processor, String charset) {
        MycatServer server = MycatServer.getContextServer();
        ConnectionManager manager = server.getConnectionManager();
    	BufferPool bufferPool = manager.getBufferPool();

    	long bufferSize = bufferPool.size();
    	long bufferCapacity = bufferPool.capacity();
    	long bufferSharedOpts = bufferPool.getSharedOptsCount();
    	long bufferUsagePercent = (bufferCapacity-bufferSize)*100/bufferCapacity;

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        try {
            row.add(processor.getName().getBytes(charset));
            row.add(LongUtil.toBytes(processor.getNetInBytes()));
            row.add(LongUtil.toBytes(processor.getNetOutBytes()));
            row.add(LongUtil.toBytes(0));
            row.add(IntegerUtil.toBytes(0));
            row.add(IntegerUtil.toBytes(processor.getWriteQueueSize()));
            row.add(LongUtil.toBytes(bufferSize));
            row.add(LongUtil.toBytes(bufferCapacity));
            row.add(LongUtil.toBytes(bufferUsagePercent));
            row.add(LongUtil.toBytes(bufferSharedOpts));
            row.add(IntegerUtil.toBytes(processor.getFrontendSize()));
            row.add(IntegerUtil.toBytes(processor.getBackendSize()));
            return row;
        } catch (final UnsupportedEncodingException e) {
            throw new FrontendException("Unsupported encoding: " + charset, e);
        }
    }

}