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
import org.opencloudb.statistic.CommandCount;
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
    // Connected count of front-ends
    protected final AtomicInteger frontendsLength = new AtomicInteger();
    protected final CommandCount commands;

    protected long netInBytes;
    protected long netOutBytes;

    public ConnectionManager(String name, BufferPool bufferPool) {
        this.name = name;
        this.bufferPool = bufferPool;

        this.frontends = new ConcurrentHashMap<>();
        this.backends = new ConcurrentHashMap<>();
        this.commands = new CommandCount();
    }

    public String getName() {
        return this.name;
    }

    public BufferPool getBufferPool() {
        return this.bufferPool;
    }

    public int getWriteQueueSize() {
        int size = 0;

        for (AbstractConnection front : this.frontends.values()) {
            size += front.getWriteQueue().size();
        }
        for (BackendConnection back : this.backends.values()) {
            size += back.getWriteQueue().size();
        }

        return size;
    }

    public CommandCount getCommands() {
        return this.commands;
    }

    public long getNetInBytes() {
        return this.netInBytes;
    }

    public void addNetInBytes(long bytes) {
        this.netInBytes += bytes;
    }

    public long getNetOutBytes() {
        return this.netOutBytes;
    }

    public void addNetOutBytes(long bytes) {
        this.netOutBytes += bytes;
    }

    public void addFrontend(FrontendConnection c) {
        this.frontends.put(c.getId(), c);
        this.frontendsLength.incrementAndGet();
    }

    public ConcurrentMap<Long, FrontendConnection> getFrontends() {
        return this.frontends;
    }

    public int getForntedsLength(){
        return this.frontendsLength.get();
    }

    public void addBackend(BackendConnection c) {
        this.backends.put(c.getId(), c);
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

            if (c.isClosed()) {
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
            FrontendConnection c = it.next().getValue();

            if (c == null) {
                it.remove();
                this.frontendsLength.decrementAndGet();
                continue;
            }

            if (c.isClosed()) {
                c.cleanup();
                it.remove();
                this.frontendsLength.decrementAndGet();
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
            this.backends.remove(con.getId());
        } else {
            this.frontends.remove(con.getId());
            this.frontendsLength.decrementAndGet();
        }
    }

    public void removeConnection(BackendConnection con){
        this.backends.remove(con.getId());
    }

    @Override
    public String toString() {
        return "ConnectionManager[name="+this.name+",netInBytes="+this.netInBytes+",netOutBytes="+this.netOutBytes+"]";
    }

}
