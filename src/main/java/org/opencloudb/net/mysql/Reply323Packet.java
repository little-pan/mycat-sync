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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.mysql.StreamUtil;
import org.opencloudb.net.BackendConnection;

/**
 * @author mycat
 */
public class Reply323Packet extends MySQLPacket {

    public byte[] seed;

    public void write(OutputStream out) throws IOException {
        StreamUtil.writeUB3(out, calcPacketSize());
        StreamUtil.write(out, packetId);
        if (seed == null) {
            StreamUtil.write(out, (byte) 0);
        } else {
            StreamUtil.writeWithNull(out, seed);
        }
    }

    @Override
    public void write(BackendConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(this.packetId);
        if (this.seed == null) {
            buffer.put((byte) 0);
        } else {
            BufferUtil.writeWithNull(buffer, this.seed);
        }
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        return seed == null ? 1 : seed.length + 1;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Auth323 Packet";
    }

}