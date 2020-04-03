package org.opencloudb.route;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.parser.druid.DruidSequenceHandler;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.Callback;
import org.slf4j.*;

public class MyCATSequenceProcessor {

	static final Logger log = LoggerFactory.getLogger(MyCATSequenceProcessor.class);
	
	public MyCATSequenceProcessor() {

	}

	public void executeSQL(final SessionSQLPair pair) {
		// 使用Druid解析器实现sequence处理  @兵临城下
		MycatServer server = MycatServer.getContextServer();
		int seqHandlerType = server.getConfig().getSystem().getSequnceHandlerType();
		DruidSequenceHandler sequenceHandler = new DruidSequenceHandler(seqHandlerType);

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
					log.error("MyCATSequenceProcessor executes 'next value' error", cause);
					source.writeErrMessage(ErrorCode.ER_YES,"MyCat sequence error: " + cause);
				}
			}
		};

		sequenceHandler.getExecuteSql(pair.sql, charset, execSQL);
	}

}
