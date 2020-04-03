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
import org.opencloudb.util.Callback;
import org.opencloudb.util.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * 本地prop文件实现递增序列号
 * 
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @time Create on 2013-12-29 23:00:05
 *
 * @version 1.0
 *
 * <p>
 *     Add those features:
 * 1. Cache and reserve an ID range for performance instead of store current ID immediately.
 * 2. Add backup mechanism for stable storage when update the properties file.
 * 3. Add optional sync operation when store the properties file.
 * </p>
 *
 * <p>
 *     Fix the bug of file handles not closed.
 * </p>
 *
 * @author little-pan
 * @since 2020-04-03
 * @version 1.5.1
 */
public class IncrSequencePropHandler extends IncrSequenceHandler {

	static final Logger log = LoggerFactory.getLogger(IncrSequencePropHandler.class);

	static final boolean ENABLE_SYNC = Boolean.getBoolean("org.opencloudb.sequence.propSeq.enableSync");
	static final String KEY_CHECKSUM_NAME = "CHECKSUM";

	private final Properties props = new Properties();
	protected final String filePath;
	private final Map<String, String> inited = new HashMap<>();
	private boolean failed;

	private static class IncrSequencePropHandlerHolder {
		static final IncrSequencePropHandler instance = new IncrSequencePropHandler();
	}

	public static IncrSequencePropHandler getInstance() {
		return IncrSequencePropHandlerHolder.instance;
	}

	private IncrSequencePropHandler() {
		this(SystemConfig.getConfigFile(FILE_NAME).getAbsolutePath());
	}

	public IncrSequencePropHandler(String filePath) {
		this.filePath = filePath;
	}

	@Override
	public void nextId(String prefixName, Callback<Long> seqCallback) {
		final long nextId, minId;

		synchronized(this) {
			loadProps();

			nextId = getCur(prefixName) + 1;
			minId = getMin(prefixName);
			boolean next = nextId >= minId;
			if (next || this.inited.get(prefixName) == null) {
				fetchNextPeriod(prefixName, next);
				nextId(prefixName, seqCallback);
				return;
			}
			updateCURIDVal(prefixName, nextId);
		}

		seqCallback.call(nextId, null);
	}

	private void loadProps() {
		if (this.props.size() > 0 && !this.failed) {
			return;
		}

		// Init
		this.failed = true;
		this.props.clear();
		this.inited.clear();

		final File backupFile = getBackupFile();
		if (backupFile.isFile()) {
			log.warn("Restore from crash: restore by using '{}'", backupFile);
			// 1. Check integrity
			Properties backProps = new Properties();
			InputStream in = null;
			try {
				in = new FileInputStream(backupFile);
				backProps.load(in);
			} catch (IOException e) {
				throw new IllegalStateException("Load '" + backupFile + "' error", e);
			} finally {
				IoUtil.close(in);
			}
			String s = (String)backProps.remove(KEY_CHECKSUM_NAME);
			long checksum = Long.parseLong(s);
			if (checksum == calcChecksum(backProps)) {
				// 2. Restore work
				log.info("Backup of '{}' is valid: use the backup", this.filePath);
				this.props.putAll(backProps);
				storeProps(this.props);
				this.failed = false;
				return;
			}

			log.info("Backup of '{}' is invalid: use the origin", this.filePath);
			if (!backupFile.delete()) {
				throw new IllegalStateException("Can't delete '" + backupFile + "'");
			}
		}

		// Do load properties
		try {
			try (FileInputStream in = new FileInputStream(this.filePath)) {
				this.props.load(in);
			}
			this.failed = false;
		} catch (IOException e) {
			throw new IllegalStateException("Load '"+this.filePath+"' error", e);
		}
	}

	private long getMax(String prefixName) {
		String s = this.props.getProperty(prefixName + KEY_MAX_NAME);
		return Long.parseLong(s);
	}

	private long getMin(String prefixName) {
		String s = this.props.getProperty(prefixName + KEY_MIN_NAME);
		return Long.parseLong(s);
	}

	private long getCur(String prefixName) {
		String s = this.props.getProperty(prefixName + KEY_CUR_NAME);
		return Long.parseLong(s);
	}

	protected void fetchNextPeriod(String prefixName, boolean next) {
		this.failed = true;
		String maxProp = prefixName + KEY_MAX_NAME;
		String minProp = prefixName + KEY_MIN_NAME;
		long minId = getMin(prefixName);
		long maxId = getMax(prefixName);
		long range = maxId - minId;

		if (range <= 0) {
			throw new IllegalStateException("'" + minProp + "' not less than '" + maxProp + "'");
		}

		final Properties stored = new Properties();
		final String curProp = prefixName + KEY_CUR_NAME;
		if (next) {
			// Init and next
			this.props.setProperty(minProp, (maxId + 1) + "");
			this.props.setProperty(maxProp, (maxId + 1 + range) + "");
			stored.putAll(this.props);
			stored.setProperty(curProp, maxId + "");
		} else {
			stored.putAll(this.props);
			// Init and the range [nextId, minId - 1] can be used
			stored.setProperty(curProp, (minId - 1) + "");
		}

		storeProps(stored);
		this.inited.put(prefixName, prefixName);
		this.failed = false;
	}

	protected void updateCURIDVal(String prefixName, long val) {
		this.failed = true;
		this.props.setProperty(prefixName + KEY_CUR_NAME, val + "");
		this.failed = false;
	}

	private void storeProps(Properties props) {
		File backupFile = getBackupFile();
		try {
			backup(backupFile, props);
			doStore(this.filePath, props);
			cleanup(backupFile);
		} catch (IOException e) {
			String s = "Store sequence properties error";
			throw new IllegalStateException(s, e);
		}
	}

	static void backup(File backupFile, Properties props) throws IOException {
		Properties backProps = new Properties();
		backProps.putAll(props);
		long checksum = calcChecksum(backProps);
		backProps.setProperty(KEY_CHECKSUM_NAME, checksum + "");

		try (FileOutputStream out = new FileOutputStream(backupFile)) {
			backProps.store(out, "");
			if (ENABLE_SYNC) out.getFD().sync();
		}
	}

	static long calcChecksum(Properties props) {
		TreeMap<String, String> sorted = new TreeMap<>();

		for (Map.Entry<Object, Object> i: props.entrySet()) {
			String name = (String)i.getKey();
			sorted.put(name, (String)i.getValue());
		}
		StringBuilder sbuf = new StringBuilder();
		for (Map.Entry<String, String> i: sorted.entrySet()) {
			sbuf.append(i.getKey()).append(i.getValue());
		}
		byte[] data = sbuf.toString().getBytes(StandardCharsets.UTF_8);
		CRC32 crc32 = new CRC32();
		crc32.update(data);

		return crc32.getValue();
	}

	static void doStore(String filePath, Properties props) throws IOException {
		try (FileOutputStream out = new FileOutputStream(filePath)) {
			props.store(out, "");
			if (ENABLE_SYNC) out.getFD().sync();
		}
	}

	static void cleanup(File backupFile) throws IOException {
		if (!backupFile.delete()) {
			throw new IOException("Can't delete '"+backupFile+"'");
		}
	}

	File getBackupFile() {
		return new File(this.filePath+".bak");
	}

}
