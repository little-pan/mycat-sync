package org.opencloudb.mpp;

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

import java.nio.ByteBuffer;
import java.util.*;

import org.opencloudb.mpp.sorter.RowSorter;
import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.mysql.handler.MultiNodeQueryHandler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.ServerSession;
import org.opencloudb.util.StringUtil;
import org.slf4j.*;

/**
 * Data merge service handle data Min, Max, AVG group-or-order by, limit
 * 
 * @author wuzhih /modify by coder_czp/2015/11/2
 * 
 */
public class DataMergeService {

	private static Logger log = LoggerFactory.getLogger(DataMergeService.class);

	private int fieldCount;
	private RouteResultset rrs;
	private RowSorter sorter;
	private RowDataPacketGrouper grouper;
	private MultiNodeQueryHandler multiQueryHandler;
	private List<RowDataPacket> result = new LinkedList<>();
	// canDiscard: node -> true (subsequent rows can be discarded when only have "ORDER BY")
	private Map<String, Boolean> canDiscard = new HashMap<>();

	public DataMergeService(MultiNodeQueryHandler handler, RouteResultset rrs) {
		this.rrs = rrs;
		this.multiQueryHandler = handler;
	}

	public RouteResultset getRrs() {
		return this.rrs;
	}

	public void outputMergeResult(ServerSession session, byte[] eof) {
		ServerConnection source = session.getSource();
		int warningCount = 0;
		List<RowDataPacket> packets;
		byte[] array;

		EOFPacket eofp = new EOFPacket();
		ByteBuffer eofb = ByteBuffer.allocate(9);
		BufferUtil.writeUB3(eofb, eofp.calcPacketSize());
		eofb.put(eofp.packetId);
		eofb.put(eofp.fieldCount);
		BufferUtil.writeUB2(eofb, warningCount);
		BufferUtil.writeUB2(eofb, eofp.status);
		array = eofb.array();
		packets = getResults(array);

		this.multiQueryHandler.outputMergeResult(source, array, packets);
	}

	public void onRowMetaData(Map<String, ColMeta> columnToIndex, int fieldCount) {
		if (log.isDebugEnabled()) {
			log.debug("Field metadata info: {}", columnToIndex.entrySet());
		}
		int[] groupColumnIndexs = null;
		this.fieldCount = fieldCount;
		if (this.rrs.getGroupByCols() != null) {
			groupColumnIndexs = toColumnIndex(rrs.getGroupByCols(), columnToIndex);
		}

		if (this.rrs.getHavingCols() != null) {
			ColMeta colMeta = columnToIndex.get(rrs.getHavingCols().getLeft().toUpperCase());
			if (colMeta != null) {
				this.rrs.getHavingCols().setColMeta(colMeta);
			}
		}

		if (this.rrs.isHasAggrColumn()) {
			List<MergeCol> mergeCols = new LinkedList<>();
			Map<String, Integer> mergeColsMap = this.rrs.getMergeCols();
			if (mergeColsMap != null) {
				for (Map.Entry<String, Integer> mergEntry : mergeColsMap
						.entrySet()) {
					String colName = mergEntry.getKey().toUpperCase();
					int type = mergEntry.getValue();
					if (MergeCol.MERGE_AVG == type) {
						ColMeta sumColMeta = columnToIndex.get(colName + "SUM");
						ColMeta countColMeta = columnToIndex.get(colName + "COUNT");
						if (sumColMeta != null && countColMeta != null) {
							ColMeta colMeta = new ColMeta(sumColMeta.colIndex,
									countColMeta.colIndex, sumColMeta.getColType());
							mergeCols.add(new MergeCol(colMeta, mergEntry.getValue()));
						}
					} else {
						ColMeta colMeta = columnToIndex.get(colName);
						mergeCols.add(new MergeCol(colMeta, mergEntry.getValue()));
					}
				}
			}
			// add no alias merge column
			for (Map.Entry<String, ColMeta> fieldEntry : columnToIndex.entrySet()) {
				String colName = fieldEntry.getKey();
				int result = MergeCol.tryParseAggCol(colName);
				if (result != MergeCol.MERGE_UNSUPPORT && result != MergeCol.MERGE_NOMERGE) {
					mergeCols.add(new MergeCol(fieldEntry.getValue(), result));
				}
			}
			this.grouper = new RowDataPacketGrouper(groupColumnIndexs,
					mergeCols.toArray(new MergeCol[0]), this.rrs.getHavingCols());
		}
		if (this.rrs.getOrderByCols() != null) {
			LinkedHashMap<String, Integer> orders = this.rrs.getOrderByCols();
			OrderCol[] orderCols = new OrderCol[orders.size()];
			int i = 0;
			for (Map.Entry<String, Integer> entry : orders.entrySet()) {
				String key = StringUtil.removeBackquote(entry.getKey().toUpperCase());
				ColMeta colMeta = columnToIndex.get(key);
				if (colMeta == null) {
					String s = "All columns in 'order by' clause " +
							"should be in the selected column list: " + entry.getKey();
					throw new IllegalArgumentException(s);
				}
				orderCols[i++] = new OrderCol(colMeta, entry.getValue());
			}
			RowSorter tmp = new RowSorter(orderCols);
			tmp.setLimit(this.rrs.getLimitStart(), this.rrs.getLimitSize());
			this.sorter = tmp;
		}
	}

	/**
	 * Process new record (mysql binary data), if sequent data can be discarded return true
	 * 
	 * @param dataNode
	 *            DN's name (data from this dataNode)
	 * @param rowData
	 *            raw data
	 */
	public boolean onNewRecord(String dataNode, byte[] rowData) {
		// 对于需要排序的数据,由于mysql传递过来的数据是有序的,
		// 如果某个节点的当前数据已经不会进入,后续的数据也不会入堆
		if (this.canDiscard.size() == this.rrs.getNodes().length) {
			// Note: now we also can't do output! Because sequent data maybe remain
			// in backend connection and this can lead to data confusion when execute
			// next query.
			return true;
		}
		if (this.canDiscard.get(dataNode) != null) {
			return true;
		}

		RowDataPacket row = new RowDataPacket(this.fieldCount);
		row.read(rowData);
		if (this.grouper != null) {
			this.grouper.addRow(row);
		} else if (this.sorter != null) {
			if (!this.sorter.addRow(row)) {
				this.canDiscard.put(dataNode, true);
			}
		} else {
			this.result.add(row);
		}

		return false;
	}

	private static int[] toColumnIndex(String[] columns, Map<String, ColMeta> toIndexMap) {
		int[] result = new int[columns.length];
		ColMeta curColMeta;
		for (int i = 0; i < columns.length; i++) {
			curColMeta = toIndexMap.get(columns[i].toUpperCase());
			if (curColMeta == null) {
				throw new java.lang.IllegalArgumentException(
						"All columns in group by clause should be in the selected column list: "
								+ columns[i]);
			}
			result[i] = curColMeta.colIndex;
		}
		return result;
	}

	/**
	 * release resources
	 */
	public void clear() {
		this.result.clear();
		this.grouper = null;
		this.sorter = null;
	}

	/**
	 * return merged data
	 * 
	 * @return (最多i*(offset+size)行数据)
	 */
	private List<RowDataPacket> getResults(byte[] eof) {
		List<RowDataPacket> tmpResult = this.result;
		if (this.grouper != null) {
			tmpResult = this.grouper.getResult();
			this.grouper = null;
		}
		if (this.sorter != null) {
			// 处理grouper处理后的数据
			if (tmpResult != null) {
				Iterator<RowDataPacket> it = tmpResult.iterator();
				while (it.hasNext()) {
					this.sorter.addRow(it.next());
					it.remove();
				}
			}
			tmpResult = this.sorter.getSortedResult();
			this.sorter = null;
		}
		if (log.isDebugEnabled()) {
			log.debug("prepare mpp merge result for '{}'", this.rrs.getStatement());
		}

		return tmpResult;
	}

}
