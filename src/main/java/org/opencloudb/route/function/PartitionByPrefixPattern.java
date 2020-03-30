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
package org.opencloudb.route.function;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.rule.RuleAlgorithm;
import org.opencloudb.util.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * partition by Prefix length ,can be used in String partition
 * 
 * @author hexiaobin
 */
public class PartitionByPrefixPattern extends AbstractPartitionAlgorithm implements RuleAlgorithm {

	static final Logger log = LoggerFactory.getLogger(PartitionByPrefixPattern.class);

	private static final int PARTITION_LENGTH = 1024;
	private int patternValue = PARTITION_LENGTH;// 分区长度，取模数值(默认为1024)
	private int prefixLength;// 字符前几位进行ASCII码取和
	private String mapFile;
	private LongRange[] longRongs;

	@Override
	public void init() {
		initialize();
	}

	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}

	public void setPatternValue(int patternValue) {
		this.patternValue = patternValue;
	}

	public void setPrefixLength(int prefixLength) {
		this.prefixLength = prefixLength;
	}

	@Override
	public Integer calculate(String columnValue) {
		int length = prefixLength;

		length = Math.min(columnValue.length(), length);
		int sum = 0;
		for (int i = 0; i < length; i++) {
			sum = sum + columnValue.charAt(i);
		}
		Integer rst = null;
		for (LongRange longRang : this.longRongs) {
			long hash = sum % patternValue;
			if (hash <= longRang.valueEnd && hash >= longRang.valueStart) {
				return longRang.nodeIndx;
			}
		}
		return rst;
	}

	private void initialize() {
		BufferedReader in = null;
		InputStream fin = getConfigFileStream(mapFile);
		try {
			in = new BufferedReader(new InputStreamReader(fin, SystemConfig.CHARSET));
			LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();

			for (String line; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//"))
					continue;
				int ind = line.indexOf('=');
				if (ind < 0) {
					log.warn("Bad line in '{}': {}", mapFile, line);
					continue;
				}
				try {
					String pairs[] = line.substring(0, ind).trim().split("-");
					long longStart = NumberParseUtil.parseLong(pairs[0].trim());
					long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
					int nodeId = Integer.parseInt(line.substring(ind + 1).trim());
					longRangeList.add(new LongRange(nodeId, longStart, longEnd));
				} catch (Exception e) {
				}
			}
			longRongs = longRangeList.toArray(new LongRange[longRangeList.size()]);
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}

		} finally {
			IoUtil.close(in);
			IoUtil.close(fin);
		}
	}

	static class LongRange {
		public final int nodeIndx;
		public final long valueStart;
		public final long valueEnd;

		public LongRange(int nodeIndx, long valueStart, long valueEnd) {
			super();
			this.nodeIndx = nodeIndx;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}

	}
}
