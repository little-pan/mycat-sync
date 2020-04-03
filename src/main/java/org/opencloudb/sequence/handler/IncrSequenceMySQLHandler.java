package org.opencloudb.sequence.handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.mysql.handler.AbstractResponseHandler;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.net.NioProcessor;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.Callback;
import org.opencloudb.util.IoUtil;
import org.slf4j.*;

/**
 * Support sequence non-blocking fetching.
 *
 * @author little-pan
 * @since 2020-04-03
 * @version 1.5.1
 */
public class IncrSequenceMySQLHandler implements SequenceHandler {

	static final Logger log = LoggerFactory.getLogger(IncrSequenceMySQLHandler.class);

	static final String SEQUENCE_DB_PROPS = "sequence_db_conf.properties";
	static final String errSeqResult = "-999999999,null";

	static class IncrSequenceMySQLHandlerHolder {
		static final IncrSequenceMySQLHandler instance = new IncrSequenceMySQLHandler();
	}

	public static IncrSequenceMySQLHandler getInstance() {
		return IncrSequenceMySQLHandlerHolder.instance;
	}

	final FetchMySQLSequenceHandler mysqlSeqFetcher = new FetchMySQLSequenceHandler();
	// Sequence -> curval
	private final ConcurrentMap<String, SequenceVal> seqValueMap = new ConcurrentHashMap<>();

	public IncrSequenceMySQLHandler() {
		load();
	}

	public void load() {
		// Load sequence properties
		Properties props = loadProps();
		removeDesertedSequenceVals(props);
		putNewSequenceVals(props);
	}

	private Properties loadProps() {
		String propsFile = SEQUENCE_DB_PROPS;
		final InputStream in = SystemConfig.getConfigFileStream(propsFile);
		final Properties props = new Properties();

		try {
			props.load(in);
			return props;
		} catch (IOException e) {
			throw new IllegalStateException("Load file '"+propsFile+"' error", e);
		} finally {
			IoUtil.close(in);
		}
	}

	private void removeDesertedSequenceVals(Properties props) {
		Iterator<Map.Entry<String, SequenceVal>> it = this.seqValueMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, SequenceVal> entry = it.next();
			if (!props.containsKey(entry.getKey())) {
				it.remove();
			}
		}
	}

	private void putNewSequenceVals(Properties props) {
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			String seqName = (String) entry.getKey();
			String dataNode = (String) entry.getValue();
			SequenceVal seqVal = this.seqValueMap.get(seqName);
			if (seqVal == null) {
				seqVal = new SequenceVal(seqName, dataNode, this);
				this.seqValueMap.put(seqName, seqVal);
			} else {
				seqVal.dataNode = dataNode;
			}
		}
	}

	@Override
	public void nextId(String seqName, Callback<Long> seqCallback) {
		final SequenceVal seqVal = this.seqValueMap.get(seqName);

		if (seqVal == null) {
			Throwable cause = new ConfigException("Can't find definition for sequence '" + seqName +"'");
			seqCallback.call(null, cause);
			return;
		}
		getNextValidSeqVal(seqVal, seqCallback);
	}

	void getNextValidSeqVal(SequenceVal seqVal, Callback<Long> seqCallback) {
		final Long nexVal = seqVal.nextValue();
		if (seqVal.isNextValValid(nexVal)) {
			seqCallback.call(nexVal, null);
			return;
		}

		getSeqValueFromDB(seqVal, seqCallback);
	}

	private void getSeqValueFromDB(SequenceVal seqVal, Callback<Long> seqCallback) {
		log.debug("Fetch next segment of sequence '{}' from DB: current value {}",
				seqVal.seqName, seqVal.curVal);

		NioProcessor processor = NioProcessor.ensureRunInProcessor();
		seqVal.fetch(processor, seqCallback);
	}

}

class SequenceVal {

	static final Logger log = LoggerFactory.getLogger(SequenceVal.class);

	public final String seqName;
	public final String sql;
	public volatile String dataNode;
	public final IncrSequenceMySQLHandler seqHandler;

	public final AtomicLong curVal = new AtomicLong();
	public volatile long maxSegValue;

	private final Queue<WaitingItem> waitingQueue = new LinkedList<>();
	private Callback<Long> fetchCallback;
	private boolean fetching;
	public String lastError;
	public String dbReturnVal = null;

	public SequenceVal(String seqName, String dataNode, IncrSequenceMySQLHandler seqHandler) {
		this.seqName = seqName;
		this.dataNode = dataNode;
		this.sql = "SELECT mycat_seq_nextval('" + seqName + "')";
		this.seqHandler = seqHandler;
	}

	public boolean isNextValValid(Long nexVal) {
		return (nexVal < this.maxSegValue);
	}

	public long nextValue() {
		return this.curVal.incrementAndGet();
	}

	void fetch(NioProcessor processor, final Callback<Long> seqCallback) {
		final SequenceVal self = this;

		synchronized (this) {
			if (this.fetching) {
				log.debug("State: waiting sequence '{}' fetched", this.seqName);
				Runnable seqTask = new Runnable() {
					@Override
					public void run() {
						seqHandler.getNextValidSeqVal(self, seqCallback);
					}
				};
				WaitingItem item = new WaitingItem(processor, seqCallback, seqTask);
				this.waitingQueue.offer(item);
				return;
			}
			log.debug("State: fetching sequence '{}'", this.seqName);
			this.fetching = true;
		}

		FetchMySQLSequenceHandler fetcher;
		boolean failed = true;
		try {
			fetcher = this.seqHandler.mysqlSeqFetcher;
			this.lastError = null;
			this.fetchCallback = seqCallback;
			this.dbReturnVal = null;
			fetcher.execute(this);
			failed = false;
		} finally {
			if (failed) {
				fetchComplete();
			}
		}
	}

	void fetchSuccess() {
		log.debug("State: fetch sequence '{}' success, return value '{}'",
				this.seqName, this.dbReturnVal);

		boolean called = false;
		try {
			if (IncrSequenceMySQLHandler.errSeqResult.equals(this.dbReturnVal)) {
				Exception cause = new ConfigException("Sequence '"+this.seqName+"' not found in db table");
				called = true;
				this.fetchCallback.call(null, cause);
			} else {
				final String[] items = this.dbReturnVal.split(",");
				final long curVal = Long.parseLong(items[0]);
				final int span = Integer.parseInt(items[1]);
				if (span <= 0) {
					String s = "Sequence '"+this.seqName+"' increment less than 1 in db table";
					Exception cause = new ConfigException(s);
					called = true;
					this.fetchCallback.call(null, cause);
				} else {
					this.curVal.set(curVal);
					this.maxSegValue = curVal + span;
					called = true;
					this.fetchCallback.call(curVal, null);
				}
			}
		} finally {
			if (called) {
				this.fetchCallback = null;
			}
			fetchComplete();
		}
	}

	void fetchComplete() {
		log.debug("State: fetch sequence '{}' complete, last error '{}'",
				this.seqName, this.lastError);

		synchronized (this) {
			try {
				// Wakeup those waiting sequence requests
				for (;;) {
					WaitingItem item = this.waitingQueue.poll();
					if (item == null) {
						break;
					}
					item.processor.execute(item.seqTask);
				}
				// Callback itself
				if (this.lastError != null && this.fetchCallback != null) {
					Exception cause = new ConfigException(this.lastError);
					this.fetchCallback.call(null, cause);
				}
			} finally {
				this.lastError = null;
				this.fetchCallback = null;
				this.fetching = false;
			}
		}
	}

	static class WaitingItem {
		public final NioProcessor processor;
		public final Callback<Long> callback;
		public final Runnable seqTask;

		public WaitingItem(NioProcessor processor, Callback<Long> callback, Runnable seqTask) {
			this.processor = processor;
			this.callback = callback;
			this.seqTask = seqTask;
		}

	}

}

class FetchMySQLSequenceHandler extends AbstractResponseHandler {

	static final Logger log = LoggerFactory.getLogger(FetchMySQLSequenceHandler.class);

	public void execute(SequenceVal seqVal) {
		log.debug("execute in datanode '{}' for fetch sequence sql '{}'", seqVal.dataNode, seqVal.sql);

		MycatServer server = MycatServer.getContextServer();
		MycatConfig conf = server.getConfig();
		PhysicalDBNode mysqlDN = conf.getDataNodes().get(seqVal.dataNode);
		// 修正获取seq的逻辑，在读写分离的情况下只能走写节点，修改Select模式为Update模式
		RouteResultsetNode rrn = new RouteResultsetNode(seqVal.dataNode, ServerParse.UPDATE, seqVal.sql);
		mysqlDN.getConnection(mysqlDN.getDatabase(), true, rrn, this, seqVal);
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		conn.setResponseHandler(this);
		try {
			SequenceVal seqVal = (SequenceVal)conn.getAttachment();
			conn.query(seqVal.sql);
		} catch (Throwable cause) {
			executeException(conn, cause);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		log.debug("Connection error", e);
		executeException(conn, e);
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		if (conn.syncAndExecute()) {
			log.debug("Sync and execute: ok");
			conn.release();
		}
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		SequenceVal seqVal = ((SequenceVal) conn.getAttachment());
		try {
			ErrorPacket err = new ErrorPacket();
			err.read(data);
			String errMsg = new String(err.message);
			seqVal.lastError = errMsg;
			log.warn("Error response: errno {}, errmsg '{}'", err.errno, errMsg);
		} finally {
			conn.release();
		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
								 byte[] eof, BackendConnection conn) {

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		try {
			RowDataPacket rowDataPkg = new RowDataPacket(1);
			rowDataPkg.read(row);
			byte[] columnData = rowDataPkg.fieldValues.get(0);
			String columnVal = new String(columnData);
			SequenceVal seqVal = (SequenceVal) conn.getAttachment();
			seqVal.dbReturnVal = columnVal;
			if (IncrSequenceMySQLHandler.errSeqResult.equals(columnVal)) {
				log.warn("Sequence sql returned error value: '{}' in sequence('{}', '{}')" ,
						columnVal, seqVal.seqName, seqVal.sql);
			}
		} catch (Throwable cause) {
			executeException(conn, cause);
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection c) {
		boolean released = false;
		try {
			SequenceVal seqVal = ((SequenceVal) c.getAttachment());
			c.release();
			released = true;
			seqVal.fetchSuccess();
		} finally {
			if (!released) {
				c.release();
			}
		}
	}

	private void executeException(BackendConnection c, Throwable e) {
		String errMgs = "Fetch sequence error";
		boolean closed = false;
		try {
			SequenceVal seqVal = ((SequenceVal) c.getAttachment());
			errMgs += ": " + e.toString();
			c.close(errMgs);
			closed = true;
			seqVal.lastError = errMgs;
			log.warn("Fetch sequence error", e);
			seqVal.fetchComplete();
		} finally {
			if (!closed) {
				c.close(errMgs);
			}
		}
	}

}
