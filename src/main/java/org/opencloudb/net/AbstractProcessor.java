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

/** The abstract processor of a front-end connection.
 *
 * @author little-pan
 * @since 2020-03-24
 */
public abstract class AbstractProcessor implements Runnable {

    public static final String PROCESSOR_THREAD_PREFIX = "$_";

    protected final String name;

    protected AbstractProcessor(String name) {
        this.name = name;
    }

    @Override
    public abstract void run();

    public String getName () {
        return this.name;
    }

    public static boolean isProcessorThread() {
        String threadName = Thread.currentThread().getName();
        return (threadName.startsWith(PROCESSOR_THREAD_PREFIX));
    }

}
