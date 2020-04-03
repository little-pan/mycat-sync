package org.opencloudb.sequence.handler;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.util.Callback;
import org.opencloudb.util.IoUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class IncrSequenceTimeHandler implements SequenceHandler {

	private static final String SEQUENCE_DB_PROPS = "sequence_time_conf.properties";
	private static final IncrSequenceTimeHandler instance = new IncrSequenceTimeHandler();

	public static IncrSequenceTimeHandler getInstance() {
		return IncrSequenceTimeHandler.instance;
	}

	private final String propsFile;
	private final boolean inConfigPath;

	private IdWorker worker;

	private IncrSequenceTimeHandler() {
		this.propsFile = SEQUENCE_DB_PROPS;
		this.inConfigPath = true;
	}

	public IncrSequenceTimeHandler(String propsFile) {
		this(propsFile, false);
	}

	public IncrSequenceTimeHandler(String propsFile, boolean load) {
		this.propsFile = propsFile;
		this.inConfigPath = false;
		if (load) {
			load();
		}
	}

	public IncrSequenceTimeHandler load() {
		// load sequence properties
		Properties props = loadProps(this.propsFile, this.inConfigPath);
		long workid = Long.parseLong(props.getProperty("WORKID"));
		long dataCenterId = Long.parseLong(props.getProperty("DATAACENTERID"));

		this.worker = new IdWorker(workid, dataCenterId);
		return this;
	}

	private Properties loadProps(String propsFile, boolean configPath){
		Properties props = new Properties();
		InputStream in = null;
		try {
			if (configPath) {
				in = SystemConfig.getConfigFileStream(propsFile);
			} else {
				in = new FileInputStream(propsFile);
			}
			props.load(in);
		} catch (IOException e) {
			throw new ConfigException("Fatal: load file '" + propsFile + "'", e);
		} finally {
			IoUtil.close(in);
		}

		return props;
	}

	@Override
	public void nextId(String prefixName, Callback<Long> seqCallback) {
		if (this.worker == null) {
			throw new IllegalStateException("Not loaded");
		}

		long seq = this.worker.nextId();
		seqCallback.call(seq, null);
	}

	/**
	* 64位ID (42(毫秒)+5(机器ID)+5(业务编码)+12(重复累加))
	* @author sw
	*/
	static class IdWorker {

		private final static long twepoch = 1288834974657L;
		// 机器标识位数
		private final static long workerIdBits = 5L;
		// 数据中心标识位数
		private final static long datacenterIdBits = 5L;
		// 机器ID最大值 31
		private final static long maxWorkerId = -1L ^ (-1L << workerIdBits);
		// 数据中心ID最大值 31
		private final static long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
		// 毫秒内自增位
		private final static long sequenceBits = 12L;
		// 机器ID偏左移12位
		private final static long workerIdShift = sequenceBits;
		// 数据中心ID左移17位
		private final static long datacenterIdShift = sequenceBits + workerIdBits;
		// 时间毫秒左移22位
		private final static long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;

		private final static long sequenceMask = -1L ^ (-1L << sequenceBits);

		private static long lastTimestamp = -1L;

		private long sequence = 0L;
		private final long workerId;
		private final long datacenterId;

		public IdWorker(long workerId, long datacenterId) {
			if (workerId > maxWorkerId || workerId < 0) {
				throw new IllegalArgumentException("worker Id can't be greater than %d or less than 0");
			}
			if (datacenterId > maxDatacenterId || datacenterId < 0) {
				throw new IllegalArgumentException("datacenter Id can't be greater than %d or less than 0");
			}
			this.workerId = workerId;
			this.datacenterId = datacenterId;
		}

		public synchronized long nextId() {
			long timestamp = timeGen();
			if (timestamp < lastTimestamp) {
				throw new IllegalStateException("Clock moved backwards. Refusing to generate id for "
						+ (lastTimestamp - timestamp) + " milliseconds");
			}

			if (lastTimestamp == timestamp) {
				// 当前毫秒内，则+1
				sequence = (sequence + 1) & sequenceMask;
				if (sequence == 0) {
					// 当前毫秒内计数满了，则等待下一秒
					timestamp = tilNextMillis(lastTimestamp);
				}
			} else {
				sequence = 0;
			}
			lastTimestamp = timestamp;
			// ID偏移组合生成最终的ID，并返回ID

			return ((timestamp - twepoch) << timestampLeftShift)
					| (datacenterId << datacenterIdShift)
					| (workerId << workerIdShift) | sequence;
		}

		private long tilNextMillis(final long lastTimestamp) {
			long timestamp = this.timeGen();
			while (timestamp <= lastTimestamp) {
				timestamp = this.timeGen();
			}
			return timestamp;
		}

		private long timeGen() {
			return System.currentTimeMillis();
		}

	}

}
