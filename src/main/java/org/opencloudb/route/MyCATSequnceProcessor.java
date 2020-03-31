package org.opencloudb.route;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.parser.druid.DruidSequenceHandler;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.StringUtil;
import org.slf4j.*;

public class MyCATSequnceProcessor {

	private static final Logger log = LoggerFactory.getLogger(MyCATSequnceProcessor.class);

	private LinkedBlockingQueue<SessionSQLPair> seqSQLQueue = new LinkedBlockingQueue<>();
	private volatile boolean running = true;
	
	public MyCATSequnceProcessor() {
		new ExecuteThread().start();
	}

	public void addNewSql(SessionSQLPair pair) {
		seqSQLQueue.add(pair);
	}

	private void outRawData(ServerConnection sc,String value) {
		byte packetId = 0;
		int fieldCount = 1;
		ByteBuffer byteBuf = sc.allocate();
		ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
		headerPkg.fieldCount = fieldCount;
		headerPkg.packetId = ++packetId;

		byteBuf = headerPkg.write(byteBuf, sc, true);
		FieldPacket fieldPkg = new FieldPacket();
		fieldPkg.packetId = ++packetId;
		fieldPkg.name = StringUtil.encode("SEQUNCE", sc.getCharset());
		byteBuf = fieldPkg.write(byteBuf, sc, true);

		EOFPacket eofPckg = new EOFPacket();
		eofPckg.packetId = ++packetId;
		byteBuf = eofPckg.write(byteBuf, sc, true);

		RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
		rowDataPkg.packetId = ++packetId;
		rowDataPkg.add(StringUtil.encode(value, sc.getCharset()));
		byteBuf = rowDataPkg.write(byteBuf, sc, true);
		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		byteBuf = lastEof.write(byteBuf, sc, true);

		// write buffer
		sc.write(byteBuf);
	}

	private void executeSeq(SessionSQLPair pair) {
		try {
			//使用Druid解析器实现sequence处理  @兵临城下
			MycatServer server = MycatServer.getContextServer();
			int seqHandlerType = server.getConfig().getSystem().getSequnceHandlerType();
			DruidSequenceHandler sequenceHandler = new DruidSequenceHandler(seqHandlerType);
			
			String charset = pair.session.getSource().getCharset();
			String executeSql = sequenceHandler.getExecuteSql(pair.sql,charset == null ? "utf-8":charset);
			
			pair.session.getSource().routeEndExecuteSQL(executeSql, pair.type,pair.schema);
		} catch (Exception e) {
			log.error("MyCATSequenceProcessor.executeSeq(SesionSQLPair)", e);
			pair.session.getSource().writeErrMessage(ErrorCode.ER_YES,"mycat sequnce err." + e);
			return;
		}
	}
	
	public void shutdown(){
		running=false;
	}
	
	class ExecuteThread extends Thread {
		{
			setName("MyCAT-seq");
			setDaemon(true);
		}

		public void run() {
			while (running) {
				try {
					SessionSQLPair pair=seqSQLQueue.poll(100, TimeUnit.MILLISECONDS);
					if(pair!=null){
						executeSeq(pair);
					}
				} catch (Exception e) {
					log.warn("MyCATSequenceProcessor$ExecutorThread", e);
				}
			}
		}
	}
}
