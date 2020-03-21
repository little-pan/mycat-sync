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

import org.opencloudb.util.ExecutorUtil;
import org.opencloudb.util.NameableExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/** The blocking processor pool of a front-end connection.
 *
 * @author little-pan
 * @since 2020-03-21
 */
public class BioProcessorPool implements AutoCloseable {

    static final Logger log = LoggerFactory.getLogger(BioProcessorPool.class);

    protected final NameableExecutor executor;

    public BioProcessorPool (String name, int coreSize, int maxSize) {
        this(name, coreSize, maxSize, false);
    }

    public BioProcessorPool (String name, int coreSize, int maxSize, boolean daemon) {
        this.executor = ExecutorUtil.create(name, coreSize, maxSize, daemon);
    }

    public boolean execute (BioProcessor processor) {
        try {
            this.executor.execute(processor);
            return true;
        } catch (final RejectedExecutionException e) {
            log.debug("Submit processor failed", e);
            return false;
        }
    }

    public void shutdown () {
        close();
    }

    public boolean join () {
        try {
            boolean result = this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            log.info("{} terminated",  this.executor.getName());
            return result;
        } catch (final InterruptedException e) {
            return false;
        }
    }

    @Override
    public void close() {
        log.info("{} close",  this.executor.getName());
        this.executor.shutdown();
    }

}
