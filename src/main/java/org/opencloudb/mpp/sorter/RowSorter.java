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

import org.opencloudb.mpp.*;
import org.opencloudb.net.mysql.RowDataPacket;

import java.util.Collections;
import java.util.List;

public class RowSorter extends RowDataPacketSorter {

    // 记录总数(=offset+limit)
    private volatile int total;
    // 查询的记录数(=limit)
    private volatile int size;
    // 堆
    private volatile HeapItf heap;
    // 多列比较器
    private volatile RowDataCmp cmp;
    // 是否执行过buildHeap
    private volatile boolean hasBuild;

    public RowSorter(OrderCol[] orderCols) {
        super(orderCols);
        this.cmp = new RowDataCmp(orderCols);
    }

    public synchronized void setLimit(int start, int size) {
        // 容错处理
        if (start < 0) {
            start = 0;
        }
        if (size <= 0) {
            this.total = this.size = Integer.MAX_VALUE;
        } else {
            this.total = start + size;
            this.size = size;
        }
        // 统一采用顺序，order by 条件交给比较器去处理
        this.heap = new MaxHeap(cmp, total);
    }

    @Override
    public synchronized boolean addRow(RowDataPacket row) {
        if (heap.getData().size() < total) {
            heap.add(row);
            return true;
        }
        // 堆已满，构建最大堆，并执行淘汰元素逻辑
        if (heap.getData().size() == total && hasBuild == false) {
            heap.buildHeap();
            hasBuild = true;
        }
        return heap.addIfRequired(row);
    }

    @Override
    public List<RowDataPacket> getSortedResult() {
        final List<RowDataPacket> data = heap.getData();
        if (data.size() < 2)
            return data;

        if (total - size > data.size())
            return Collections.emptyList();

        // 构建最大堆并排序
        if (!hasBuild) {
            heap.buildHeap();
        }
        heap.heapSort(this.size);
        return heap.getData();
    }

    public RowDataCmp getCmp() {
        return cmp;
    }

}
