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
package org.opencloudb.sequence.handler;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.util.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 本地prop文件实现递增序列号
 * 
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @time Create on 2013-12-29 下午11:00:05
 * @version 1.0
 */
public class IncrSequencePropHandler extends IncrSequenceHandler {

	static final Logger log = LoggerFactory.getLogger(IncrSequencePropHandler.class);

	private String filePath;

	private static class IncrSequencePropHandlerHolder {
		private static final IncrSequencePropHandler instance = new IncrSequencePropHandler();
	}

	public static IncrSequencePropHandler getInstance() {
		return IncrSequencePropHandlerHolder.instance;
	}

	private IncrSequencePropHandler() {
		this.filePath = SystemConfig.getConfigFile(FILE_NAME).getAbsolutePath();
	}

	@Override
	public Map<String, String> getParaValMap(String prefixName) {
		try {
			Map<String, String> valMap = new HashMap<>();
			Properties prop = new Properties();

			prop.load(new FileInputStream(this.filePath));
			valMap.put(prefixName + KEY_HIS_NAME,
					prop.getProperty(prefixName + KEY_HIS_NAME));
			valMap.put(prefixName + KEY_MIN_NAME,
					prop.getProperty(prefixName + KEY_MIN_NAME));
			valMap.put(prefixName + KEY_MAX_NAME,
					prop.getProperty(prefixName + KEY_MAX_NAME));
			valMap.put(prefixName + KEY_CUR_NAME,
					prop.getProperty(prefixName + KEY_CUR_NAME));

			return valMap;
		} catch (Exception e) {
			throw new ConfigException("Load '"+this.filePath+"' error", e);
		}
	}

	@Override
	public Boolean fetchNextPeriod(String prefixName) {
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(filePath));
			String minStr = props.getProperty(prefixName + KEY_MIN_NAME);
			String maxStr = props.getProperty(prefixName + KEY_MAX_NAME);
			String hisIDS = props.getProperty(prefixName + KEY_HIS_NAME);
			props.setProperty(prefixName + KEY_HIS_NAME,
					"".equals(hisIDS) ? minStr + "-" + maxStr : "," + minStr
							+ "-" + maxStr);
			long minId = Long.parseLong(minStr);
			long maxId = Long.parseLong(maxStr);
			props.setProperty(prefixName + KEY_MIN_NAME, (maxId + 1) + "");
			props.setProperty(prefixName + KEY_MAX_NAME,
					(maxId - minId + maxId + 1) + "");
			props.setProperty(prefixName + KEY_CUR_NAME, maxStr);
			OutputStream fos = new FileOutputStream(filePath);
			props.store(fos, "");
		} catch (Exception e) {
			log.error("Fetch next ID period failed", e);
			return false;
		}
		return true;
	}

	@Override
	public Boolean updateCURIDVal(String prefixName, Long val) {
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(filePath));
			props.setProperty(prefixName + KEY_CUR_NAME, val + "");
			OutputStream fos = new FileOutputStream(filePath);
			props.store(fos, "");
		} catch (Exception e) {
			log.error("Update ID value failed", e);
			return false;
		}
		return true;
	}

}
