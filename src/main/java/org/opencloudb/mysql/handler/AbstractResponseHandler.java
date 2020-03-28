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
package org.opencloudb.mysql.handler;

import org.opencloudb.net.BackendConnection;

import java.util.List;

/** Simplify the implementation of response handler, and ensure that calling irrelevant method
 * throws {@link java.lang.UnsupportedOperationException} for fast-failure mode.
 *
 * @author little-pan
 * @since 2020-03-28
 */
public abstract class AbstractResponseHandler implements ResponseHandler {

    protected AbstractResponseHandler() {

    }

    /**
     * Handle the acquired connection.
     */
    @Override
    public void connectionAcquired(BackendConnection conn) {
        if (conn != null) {
            conn.close("connectionAcquired() not impl or the calling issue");
        }
        throw new UnsupportedOperationException("connectionError()");
    }

    /**
     * Handle the connection error
     *
     * @param e
     * @param conn
     */
    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        if (conn != null) {
            conn.close("connectionError(): " + e);
        }
        throw new UnsupportedOperationException("connectionError()");
    }

    /**
     * Handle the OK packet
     */
    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (conn != null) {
            conn.close("okResponse() not impl or the calling issue");
        }
        throw new UnsupportedOperationException("okResponse()");
    }

    /**
     * Handle the ERROR packet
     */
    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        if (conn != null) {
            conn.close("errorResponse() not impl or the calling issue");
        }
        throw new UnsupportedOperationException("errorResponse()");
    }

    /**
     * Handle the field eof packet
     */
    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof,
                          BackendConnection conn) {
        if (conn != null) {
            conn.close("fieldEofResponse() not impl or the calling issue");
        }
        throw new UnsupportedOperationException("fieldEofResponse()");
    }

    /**
     * Handle the row packet
     */
    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
        if (conn != null) {
            conn.close("rowResponse() not impl or the calling issue");
        }
        throw new UnsupportedOperationException("rowResponse()");
    }

    /**
     * Handle the row eof packet
     */
    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {
        if (conn != null) {
            conn.close("rowEofResponse() not impl or the calling issue");
        }
        throw new UnsupportedOperationException("rowEofResponse()");
    }

    /**
     * Handle the connection close event when backend connection closed.
     * Note: this method may be called by timer executor.
     */
    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        // NOOP default
    }

}
