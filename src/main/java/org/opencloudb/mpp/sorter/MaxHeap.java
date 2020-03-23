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

import org.opencloudb.net.mysql.RowDataPacket;

import java.util.ArrayList;
import java.util.List;

/**
 * 最大堆排序，适用于顺序排序
 *
 * @author coderczp-2014-12-8
 */
public class MaxHeap implements HeapItf {

    private RowDataCmp cmp;
    private List<RowDataPacket> data;

    public MaxHeap(RowDataCmp cmp, int size) {
        this.cmp = cmp;
        this.data = new ArrayList<>();
    }

    @Override
    public void buildHeap() {
        int len = data.size();
        for (int i = len / 2 - 1; i >= 0; i--) {
            heapifyRecursive(i, len);
        }
    }

    private void heapify(int i, int size) {
        int max = 0;
        int mid = size >> 1;// ==size/2
        while (i <= mid) {
            max = i;
            int left = i << 1;
            int right = left + 1;
            if (left < size && cmp.compare(data.get(left), data.get(i)) > 0) {
                max = left;
            }
            if (right < size && cmp.compare(data.get(right), data.get(max)) > 0) {
                max = right;
            }
            if (i == max)
                break;
            if (i != max) {
                RowDataPacket tmp = data.get(i);
                data.set(i, data.get(max));
                data.set(max, tmp);
                i = max;
            }
        }

    }

    // 递归版本
    protected void heapifyRecursive(int i, int size) {
        int l = left(i);
        int r = right(i);
        int max = i;
        if (l < size && cmp.compare(data.get(l), data.get(i)) > 0)
            max = l;
        if (r < size && cmp.compare(data.get(r), data.get(max)) > 0)
            max = r;
        if (i == max)
            return;
        swap(i, max);
        heapifyRecursive(max, size);
    }


    private int right(int i) {
        return (i + 1) << 1;
    }

    private int left(int i) {
        return ((i + 1) << 1) - 1;
    }

    private void swap(int i, int j) {
        RowDataPacket tmp = data.get(i);
        RowDataPacket elementAt = data.get(j);
        data.set(i, elementAt);
        data.set(j, tmp);
    }

    @Override
    public RowDataPacket getRoot() {
        return data.get(0);
    }

    @Override
    public void setRoot(RowDataPacket root) {
        data.set(0, root);
        heapifyRecursive(0, data.size());
    }

    @Override
    public List<RowDataPacket> getData() {
        return data;
    }

    @Override
    public void add(RowDataPacket row) {
        data.add(row);
    }

    @Override
    public boolean addIfRequired(RowDataPacket row) {
        // 淘汰堆里最小的数据
        RowDataPacket root = getRoot();
        if (cmp.compare(row, root) < 0) {
            setRoot(row);
            return true;
        }
        return false;
    }

    @Override
    public void heapSort(int size) {
        final int total = data.size();
        // 容错处理
        if (size <= 0 || size > total) {
            size = total;
        }
        final int min = size == total ? 0 : (total - size - 1);

        // 末尾与头交换，交换后调整最大堆
        for (int i = total - 1; i > min; i--) {
            swap(0, i);
            heapifyRecursive(0, i);
        }
    }

}
