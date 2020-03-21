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
package org.opencloudb.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author mycat
 */
public class NameableExecutor extends ThreadPoolExecutor {

    private static final String KEEP_ALIVE_TIME_PROP = "org.opencloudb.util.executor.keepAliveTime";
    private static final long KEEP_ALIVE_TIME = Long.getLong(KEEP_ALIVE_TIME_PROP, 60000L);

    protected String name;

    public NameableExecutor(String name, int size, BlockingQueue<Runnable> queue, ThreadFactory factory) {
        this(name, size, size, queue, factory);
    }

    public NameableExecutor(String name, int coreSize, int maxSize,
                            BlockingQueue<Runnable> queue, ThreadFactory factory) {
        super(coreSize, maxSize, KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS, queue, factory);
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
