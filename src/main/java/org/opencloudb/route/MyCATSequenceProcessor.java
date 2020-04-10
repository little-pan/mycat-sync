package org.opencloudb.route;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.parser.druid.DruidSequenceHandler;
import org.opencloudb.sequence.handler.IncrSequenceMySQLHandler;
import org.opencloudb.sequence.handler.IncrSequencePropHandler;
import org.opencloudb.sequence.handler.IncrSequenceTimeHandler;
import org.opencloudb.sequence.handler.SequenceHandler;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.Callback;
import org.opencloudb.util.ExceptionUtil;
import org.slf4j.*;

public class MyCATSequenceProcessor {

	static final Logger log = LoggerFactory.getLogger(MyCATSequenceProcessor.class);
	
	public MyCATSequenceProcessor() {

	}

	public SequenceHandler getSequenceHandler() {
		MycatServer server = MycatServer.getContextServer();
		SystemConfig config = server.getConfig().getSystem();
		int seqHandlerType = config.getSequnceHandlerType();

		return getSequenceHandler(seqHandlerType);
	}

	public static SequenceHandler getSequenceHandler(int seqHandlerType) {
		switch(seqHandlerType){
			case SystemConfig.SEQUENCEHANDLER_MYSQLDB:
				return IncrSequenceMySQLHandler.getInstance();
			case SystemConfig.SEQUENCEHANDLER_LOCALFILE:
				return IncrSequencePropHandler.getInstance();
			case SystemConfig.SEQUENCEHANDLER_LOCAL_TIME:
				return IncrSequenceTimeHandler.getInstance();
			default:
				throw new IllegalArgumentException("Invalid sequence handler type: " + seqHandlerType);
		}
	}

	public void nextValue(String table, Callback<Long> complete) {
		SequenceHandler handler = getSequenceHandler();
		handler.nextId(table, complete);
	}

	public void executeSQL(final SessionSQLPair pair) {
		// 使用Druid解析器实现sequence处理  @兵临城下
		SequenceHandler handler = getSequenceHandler();
		DruidSequenceHandler sequenceHandler = new DruidSequenceHandler(handler);

		String charset = pair.session.getSource().getCharset();
		if (charset == null) {
			charset = "utf-8";
		}

		final ServerConnection source = pair.session.getSource();
		Callback<String> execSQL = new Callback<String>() {
			@Override
			public void call(String executeSql, Throwable cause) {
				if (cause == null) {
					log.debug("execute sql with sequence '{}'", executeSql);
					source.routeEndExecuteSQL(executeSql, pair.type, pair.schema);
				} else {
					log.warn("Executes 'next value' error", cause);
					String s = ExceptionUtil.getClientMessage(cause);
					source.writeErrMessage(ErrorCode.ER_YES, s);
				}
			}
		};

		sequenceHandler.getExecuteSql(pair.sql, charset, execSQL);
	}

}
