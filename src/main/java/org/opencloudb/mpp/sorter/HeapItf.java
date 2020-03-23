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

import java.util.List;

/**
 * @author coderczp-2014-12-17
 */
public interface HeapItf {

    /**
     * 构建堆
     */
    void buildHeap();

    /**
     * 获取堆根节点
     *
     * @return
     */
    RowDataPacket getRoot();

    /**
     * 向堆添加元素
     *
     * @param row
     */
    void add(RowDataPacket row);

    /**
     * 获取堆数据
     *
     * @return
     */
    List<RowDataPacket> getData();

    /**
     * 设置根节点元素
     *
     * @param root
     */
    void setRoot(RowDataPacket root);

    /**
     * 向已满的堆添加元素
     *
     * @param row
     */
    boolean addIfRequired(RowDataPacket row);

    /**
     * 堆排序
     */
    void heapSort(int size);

}
