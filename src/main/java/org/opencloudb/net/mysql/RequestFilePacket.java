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

import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.net.FrontendConnection;

import java.nio.ByteBuffer;

/**
 * Handle the statement of 'load data local infile', used to
 * request client's file.
 */
public class RequestFilePacket extends MySQLPacket {

    public static final byte FIELD_COUNT = (byte) 251;

    public byte command = FIELD_COUNT;
    public byte[] fileName;

    @Override
    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c, boolean writeSocketIfFull) {
        int size = calcPacketSize();
        int length = c.getPacketHeaderSize() + size;

        buffer = c.checkWriteBuffer(buffer, length, writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(this.packetId);
        buffer.put(this.command);
        if (this.fileName != null) {
            buffer.put(this.fileName);
        }
        c.write(buffer);

        return buffer;
    }

    @Override
    public int calcPacketSize() {
        return (this.fileName == null ? 1 : 1 + this.fileName.length);
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Request File Packet";
    }

}
