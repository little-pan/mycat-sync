package org.opencloudb.sequence.handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.mysql.handler.AbstractResponseHandler;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;
import org.slf4j.*;

public class IncrSequenceMySQLHandler implements SequenceHandler {

	protected static final Logger log = LoggerFactory.getLogger(IncrSequenceMySQLHandler.class);

	private static final String SEQUENCE_DB_PROPS = "sequence_db_conf.properties";
	protected static final String errSeqResult = "-999999999,null";
	protected static Map<String, String> latestErrors = new ConcurrentHashMap<String, String>();
	private final FetchMySQLSequenceHandler mysqlSeqFetcher = new FetchMySQLSequenceHandler();

	private static class IncrSequenceMySQLHandlerHolder {
		private static final IncrSequenceMySQLHandler instance = new IncrSequenceMySQLHandler();
	}

	public static IncrSequenceMySQLHandler getInstance() {
		return IncrSequenceMySQLHandlerHolder.instance;
	}

	public IncrSequenceMySQLHandler() {

		load();
	}

	public void load() {
		// load sequnce properties
		Properties props = loadProps(SEQUENCE_DB_PROPS);
		removeDesertedSequenceVals(props);
		putNewSequenceVals(props);
	}

	private Properties loadProps(String propsFile) {

		Properties props = new Properties();
		InputStream inp = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(propsFile);

		if (inp == null) {
			throw new java.lang.RuntimeException(
					"db sequnce properties not found " + propsFile);

		}
		try {
			props.load(inp);
		} catch (IOException e) {
			throw new java.lang.RuntimeException(e);
		}

		return props;
	}

	private void removeDesertedSequenceVals(Properties props) {
		Iterator<Map.Entry<String, SequenceVal>> i = seqValueMap.entrySet()
				.iterator();
		while (i.hasNext()) {
			Map.Entry<String, SequenceVal> entry = i.next();
			if (!props.containsKey(entry.getKey())) {
				i.remove();
			}
		}
	}

	private void putNewSequenceVals(Properties props) {
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			String seqName = (String) entry.getKey();
			String dataNode = (String) entry.getValue();
			if (!seqValueMap.containsKey(seqName)) {
				seqValueMap.put(seqName, new SequenceVal(seqName, dataNode));
			} else {
				seqValueMap.get(seqName).dataNode = dataNode;
			}
		}
	}

	/**
	 * save sequnce -> curval
	 */
	private ConcurrentHashMap<String, SequenceVal> seqValueMap = new ConcurrentHashMap<String, SequenceVal>();

	@Override
	public long nextId(String seqName) {
		SequenceVal seqVal = seqValueMap.get(seqName);
		if (seqVal == null) {
			throw new ConfigException("can't find definition for sequence :"
					+ seqName);
		}
		if (!seqVal.isSuccessFetched()) {
			return getSeqValueFromDB(seqVal);
		} else {
			return getNextValidSeqVal(seqVal);
		}

	}

	private Long getNextValidSeqVal(SequenceVal seqVal) {
		Long nexVal = seqVal.nextValue();
		if (seqVal.isNexValValid(nexVal)) {
			return nexVal;
		} else {
			seqVal.fetching.compareAndSet(true, false);
			return getSeqValueFromDB(seqVal);
		}
	}

	private long getSeqValueFromDB(SequenceVal seqVal) {
		log.debug("get next segment of sequence from db " +
				"for sequence '{}' , curVal {}", seqVal.seqName, seqVal.curVal);

		if (seqVal.fetching.compareAndSet(false, true)) {
			seqVal.dbretVal = null;
			seqVal.dbfinished = false;
			seqVal.newValueSetted.set(false);
			mysqlSeqFetcher.execute(seqVal);
		}
		Long[] values = seqVal.waitFinish();
		if (values == null) {
			throw new RuntimeException("can't fetch sequnce in db, sequence:"
					+ seqVal.seqName + " detail:"
					+ mysqlSeqFetcher.getLastestError(seqVal.seqName));
		} else {
			if (seqVal.newValueSetted.compareAndSet(false, true)) {
				seqVal.setCurValue(values[0]);
				seqVal.maxSegValue = values[1];
				return values[0];
			} else {
				return seqVal.nextValue();
			}

		}

	}
}

class FetchMySQLSequenceHandler extends AbstractResponseHandler {

	private static final Logger log = LoggerFactory.getLogger(FetchMySQLSequenceHandler.class);

	public void execute(SequenceVal seqVal) {
		MycatServer server = MycatServer.getContextServer();
		MycatConfig conf = server.getConfig();
		PhysicalDBNode mysqlDN = conf.getDataNodes().get(seqVal.dataNode);
		try {
			log.debug("execute in datanode '{}' for fetch sequence sql '{}'", seqVal.dataNode, seqVal.sql);
			// 修正获取seq的逻辑，在读写分离的情况下只能走写节点。修改Select模式为Update模式。
			mysqlDN.getConnection(mysqlDN.getDatabase(), true,
					new RouteResultsetNode(seqVal.dataNode, ServerParse.UPDATE,
							seqVal.sql), this, seqVal);
		} catch (Exception e) {
			log.warn("get connection error", e);
		}
	}

	public String getLastestError(String seqName) {
		return IncrSequenceMySQLHandler.latestErrors.get(seqName);
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		conn.setResponseHandler(this);
		try {
			conn.query(((SequenceVal) conn.getAttachment()).sql);
		} catch (Exception e) {
			executeException(conn, e);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		((SequenceVal) conn.getAttachment()).dbfinished = true;
		log.warn("connectionError", e);
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		SequenceVal seqVal = ((SequenceVal) conn.getAttachment());
		try {
			seqVal.dbfinished = true;
			ErrorPacket err = new ErrorPacket();
			err.read(data);
			String errMsg = new String(err.message);
			log.warn("errorResponse: errno {}, errmsg '{}'", err.errno, errMsg);
			IncrSequenceMySQLHandler.latestErrors.put(seqVal.seqName, errMsg);
		} finally {
			conn.release();
		}
	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExecute();
		if (executeResponse) {
			((SequenceVal) conn.getAttachment()).dbfinished = true;
			conn.release();
		}
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		RowDataPacket rowDataPkg = new RowDataPacket(1);
		rowDataPkg.read(row);
		byte[] columnData = rowDataPkg.fieldValues.get(0);
		String columnVal = new String(columnData);
		SequenceVal seqVal = (SequenceVal) conn.getAttachment();
		if (IncrSequenceMySQLHandler.errSeqResult.equals(columnVal)) {
			seqVal.dbretVal = IncrSequenceMySQLHandler.errSeqResult;
			log.warn("sequence sql returned error value: sequence '{}', columnVal {}, sql '{}'" ,
					seqVal.seqName, columnVal, seqVal.sql);
		} else {
			seqVal.dbretVal = columnVal;
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		((SequenceVal) conn.getAttachment()).dbfinished = true;
		conn.release();
	}

	private void executeException(BackendConnection c, Throwable e) {
		SequenceVal seqVal = ((SequenceVal) c.getAttachment());
		seqVal.dbfinished = true;
		String errMgs=e.toString();
		IncrSequenceMySQLHandler.latestErrors.put(seqVal.seqName, errMgs);
		log.warn("executeException: {}", errMgs);
		c.close("exception:" +errMgs);
	}

}

class SequenceVal {

	static final Logger log = LoggerFactory.getLogger(SequenceVal.class);

	public AtomicBoolean newValueSetted = new AtomicBoolean(false);
	public AtomicLong curVal = new AtomicLong(0);
	public volatile String dbretVal = null;
	public volatile boolean dbfinished;
	public AtomicBoolean fetching = new AtomicBoolean(false);
	public volatile long maxSegValue;
	public volatile boolean successFetched;
	public volatile String dataNode;
	public final String seqName;
	public final String sql;

	public SequenceVal(String seqName, String dataNode) {
		this.seqName = seqName;
		this.dataNode = dataNode;
		sql = "SELECT mycat_seq_nextval('" + seqName + "')";
	}

	public boolean isNexValValid(Long nexVal) {
		if (nexVal < this.maxSegValue) {
			return true;
		} else {
			return false;
		}
	}

	public void setCurValue(long newValue) {
		curVal.set(newValue);
		successFetched = true;
	}

	public Long[] waitFinish() {
		long start = System.currentTimeMillis();
		long end = start + 10 * 1000;
		while (System.currentTimeMillis() < end) {
			if (dbretVal.equals(IncrSequenceMySQLHandler.errSeqResult)) {
				throw new RuntimeException("Sequence not found in db table");
			} else if (dbretVal != null) {
				String[] items = dbretVal.split(",");
				long curVal = Long.parseLong(items[0]);
				int span = Integer.parseInt(items[1]);
				return new Long[] { curVal, curVal + span };
			} else {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					log.warn("wait db fetch sequence error", e);
				}
			}
		}
		return null;
	}

	public boolean isSuccessFetched() {
		return successFetched;
	}

	public long nextValue() {
		if (!successFetched) {
			throw new RuntimeException("Sequence fetched failed  from db");
		}

		return curVal.incrementAndGet();
	}

}
