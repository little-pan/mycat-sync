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
package org.opencloudb.net.mysql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.mysql.StreamUtil;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.BackendConnection;

/**
 * @author mycat
 */
public class BinaryPacket extends MySQLPacket {

    public static final byte OK = 1;
    public static final byte ERROR = 2;
    public static final byte HEADER = 3;
    public static final byte FIELD = 4;
    public static final byte FIELD_EOF = 5;
    public static final byte ROW = 6;
    public static final byte PACKET_EOF = 7;

    public byte[] data;

    public void read(InputStream in) throws IOException {
        this.packetLength = StreamUtil.readUB3(in);
        this.packetId = StreamUtil.read(in);
        byte[] ab = new byte[this.packetLength];
        StreamUtil.read(in, ab, 0, ab.length);
        this.data = ab;
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c,boolean writeSocketIfFull) {
        buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize(),writeSocketIfFull);
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer = c.writeToBuffer(data, buffer);
        return buffer;
    }

    @Override
    public void write(BackendConnection c) {
        ByteBuffer buffer = c.allocate();
        int size = c.getPacketHeaderSize() + calcPacketSize();
        buffer = c.checkWriteBuffer(buffer, size, false);
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(this.packetId);
        buffer.put(this.data);
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        return data == null ? 0 : data.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Binary Packet";
    }

}