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

import org.opencloudb.MycatServer;
import org.opencloudb.buffer.BufferPool;
import org.opencloudb.util.TimeUtil;
import org.slf4j.*;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** The front-end and backend connection manager.
 *
 * @author little-pan
 * @since 2020-03-21
 */
public class ConnectionManager {

    static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    protected final String name;
    protected final BufferPool bufferPool;

    protected final ConcurrentMap<Long, FrontendConnection> frontends;
    protected final ConcurrentMap<Long, BackendConnection> backends;

    public ConnectionManager(String name, BufferPool bufferPool) {
        this.name = name;
        this.bufferPool = bufferPool;
        this.frontends = new ConcurrentHashMap<>();
        this.backends = new ConcurrentHashMap<>();
    }

    public String getName() {
        return this.name;
    }

    public BufferPool getBufferPool() {
        return this.bufferPool;
    }

    public ConcurrentMap<Long, FrontendConnection> getFrontends() {
        return this.frontends;
    }

    public ConcurrentMap<Long, BackendConnection> getBackends() {
        return this.backends;
    }

    /**
     * Recycle resources by interval calling.
     */
    public void checkBackendCons() {
        MycatServer server = MycatServer.getContextServer();
        long sqlTimeout = server.getConfig().getSystem().getSqlExecuteTimeout() * 1000L;
        Iterator<Map.Entry<Long, BackendConnection>> it = this.backends.entrySet().iterator();
        while (it.hasNext()) {
            BackendConnection c = it.next().getValue();

            if (c == null) {
                it.remove();
                continue;
            }

            // Close the connection when SQL execution timeout in it
            if (c.isBorrowed() && c.getLastTime() < TimeUtil.currentTimeMillis() - sqlTimeout) {
                log.warn("Found backend connection SQL timeout, close it {}", c);
                c.close("sql timeout");
            }
            final NioProcessor processor = c.getProcessor();
            if (processor != null && !processor.isOpen()) {
                c.setProcessor(null);
                c.close("Processor has been closed");
            }

            if (c.isClosed()) {
                c.cleanup();
                it.remove();
            } else {
                idleCheck(c);
            }
        }
    }

    /**
     * Recycle resources by interval calling.
     */
    public void checkFrontCons() {
        Iterator<Map.Entry<Long, FrontendConnection>> it = this.frontends.entrySet().iterator();
        while (it.hasNext()) {
            final FrontendConnection c = it.next().getValue();

            if (c == null) {
                it.remove();
                continue;
            }

            final NioProcessor processor = c.getProcessor();
            if (processor != null && !processor.isOpen()) {
                c.setProcessor(null);
                c.close("Processor has been closed");
            }

            if (c.isClosed()) {
                c.cleanup();
                it.remove();
                if (processor != null) {
                    AtomicInteger count = processor.getFrontendCount();
                    count.decrementAndGet();
                }
            } else {
                idleCheck(c);
            }
        }
    }

    protected void idleCheck(ClosableConnection c) {
        c.idleCheck();
    }

    public void removeConnection(AbstractConnection con) {
        if (con instanceof BackendConnection) {
            log.debug("{}: remove backend {}", this.name, con);
            this.backends.remove(con.getId());
        } else if (con instanceof FrontendConnection) {
            log.debug("{}: remove front {}", this.name, con);
            this.frontends.remove(con.getId());
            final NioProcessor processor = con.getProcessor();
            if (processor != null) {
                AtomicInteger count = processor.getFrontendCount();
                count.decrementAndGet();
            }
        } else {
            String s = "Passed connection not a frontend or backend connection";
            throw new IllegalArgumentException(s);
        }
    }

    public void removeConnection(BackendConnection con) {
        this.backends.remove(con.getId());
    }

    public int getWriteQueueSize(NioProcessor processor) {
        int n = 0;

        for (AbstractConnection front : this.frontends.values()) {
            if (front.getProcessor() == processor) {
                n += front.getWriteQueueSize();
            }
        }
        for (BackendConnection back : this.backends.values()) {
            if (back.getProcessor() == processor) {
                n += back.getWriteQueueSize();
            }
        }
        return n;
    }

    public int getFrontendSize(NioProcessor processor) {
        int n = 0;

        for (AbstractConnection front : this.frontends.values()) {
            if (front.getProcessor() == processor) {
                ++n;
            }
        }
        return n;
    }

    public int getBackendSize(NioProcessor processor) {
        int n = 0;

        for (BackendConnection back : this.backends.values()) {
            if (back.getProcessor() == processor) {
                ++n;
            }
        }
        return n;
    }

    @Override
    public String toString() {
        return "ConnectionManager[name="+this.name+"]";
    }

}
