/*
 * Copyright (c) 2014, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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
package org.opencloudb.mpp.sorter;

import org.opencloudb.mpp.OrderCol;
import org.opencloudb.mpp.RowDataPacketSorter;
import org.opencloudb.net.mysql.RowDataPacket;

import java.util.Comparator;

/**
 *
 * @author coderczp-2014-12-8
 */
public class RowDataCmp implements Comparator<RowDataPacket> {

    private OrderCol[] orderCols;

    public RowDataCmp(OrderCol[] orderCols) {
        this.orderCols = orderCols;
    }

    @Override
    public int compare(RowDataPacket o1, RowDataPacket o2) {
        OrderCol[] tmp = this.orderCols;
        int cmp = 0;
        int len = tmp.length;
        //依次比较order by语句上的多个排序字段的值
        int type = OrderCol.COL_ORDER_TYPE_ASC;
        for (int i = 0; i < len; i++) {
            int colIndex = tmp[i].colMeta.colIndex;
            byte[] left = o1.fieldValues.get(colIndex);
            byte[] right = o2.fieldValues.get(colIndex);
            if (tmp[i].orderType == type) {
                cmp = RowDataPacketSorter.compareObject(left, right, tmp[i]);
            } else {
                cmp = RowDataPacketSorter.compareObject(right, left, tmp[i]);
            }
            if (cmp != 0)
                return cmp;
        }
        return cmp;
    }

}
