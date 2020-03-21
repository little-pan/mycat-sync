/*
 * Copyright (c) 2020, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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
package org.opencloudb.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/** The blocking processor of a front-end connection.
 *
 * @author little-pan
 * @since 2020-03-21
 */
public class BioProcessor implements Runnable {

    static final Logger log = LoggerFactory.getLogger(BioProcessor.class);

    protected final String name;
    protected final FrontendConnection source;

    public BioProcessor(FrontendConnection source) {
        this("BioProcessor-" + source.getId(), source);
    }

    public BioProcessor(String name, FrontendConnection source) {
        this.name = name;
        this.source = source;
    }

    @Override
    public void run () {
        final FrontendConnection con = this.source;
        SocketChannel ch = (SocketChannel)con.channel;

        log.debug("{} started", this.name);
        for (;!con.isClosed();) {
            try {
                ByteBuffer buf = con.readBuffer;
                if (buf == null) {
                    buf = con.processor.getBufferPool().allocate();
                    con.readBuffer = buf;
                }
                final int n = ch.read(buf);
                log.debug("{} read {} bytes", this.name, n);
                con.onReadData(n);
            } catch (final Throwable e) {
                log.debug("{} process failed", e);
                con.close("Program error:" + e);
            }
        }
    }

    public String getName () {
        return this.name;
    }

}
