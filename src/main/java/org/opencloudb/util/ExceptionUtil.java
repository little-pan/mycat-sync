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
package org.opencloudb.util;

/** An exception handling util.
 *
 * @author little-pan
 * @since 2020-04-04
 */
public final class ExceptionUtil {

    private ExceptionUtil(){
        // NOOP
    }

    public static String getClientMessage(Throwable cause) {
        String s = cause.getMessage(), m;

        if (s == null) {
            Class<?> clazz = cause.getClass();
            return clazz.getSimpleName();
        } else {
            Throwable parent = cause.getCause();
            if (parent == null || (m = parent.getMessage()) == null) {
                return s;
            }
            return s + ": " + m;
        }
    }

}
