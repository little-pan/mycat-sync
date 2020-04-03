package org.opencloudb.parser.druid;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.sequence.handler.IncrSequenceTimeHandler;
import org.opencloudb.sequence.handler.IncrSequenceMySQLHandler;
import org.opencloudb.sequence.handler.IncrSequencePropHandler;
import org.opencloudb.sequence.handler.SequenceHandler;
import org.opencloudb.util.Callback;

/**
 * 使用Druid解析器实现对Sequence处理
 *
 * @author 兵临城下
 * @date 2015/03/13
 */
public class DruidSequenceHandler {

	// fix the issue: 'select NEXT VALUE...' -> 'selectSEQUENCE'
	static final String SEQ_REGEX = "(?:(next\\s+value\\s+for\\s*MYCATSEQ_(\\w+))(,|\\)|\\s)*)+";
	static final Pattern SEQ_PATTERN = Pattern.compile(SEQ_REGEX, Pattern.CASE_INSENSITIVE);

	private final SequenceHandler sequenceHandler;
	
	public DruidSequenceHandler(int seqHandlerType) {
		switch(seqHandlerType){
		case SystemConfig.SEQUENCEHANDLER_MYSQLDB:
			this.sequenceHandler = IncrSequenceMySQLHandler.getInstance();
			break;
		case SystemConfig.SEQUENCEHANDLER_LOCALFILE:
			this.sequenceHandler = IncrSequencePropHandler.getInstance();
			break;
		case SystemConfig.SEQUENCEHANDLER_LOCAL_TIME:
			this.sequenceHandler = IncrSequenceTimeHandler.getInstance();
			break;
		default:
			throw new IllegalArgumentException("Invalid sequence handler type: " + seqHandlerType);
		}
	}

	/**
	 * 根据原sql获取可执行的sql
	 *
	 * @param sql origin SQL statement
	 * @param charset frontend charset
	 * @param callback the success or error callback of this method
	 */
	public void getExecuteSql(final String sql, String charset, final Callback<String> callback) {
		if (null == sql || "".equals(sql)) {
			callback.call(sql, null);
			return;
		}

		// sql不能转大写，因为sql可能是insert语句会把values也给转换了获取表名
		final Matcher matcher = SEQ_PATTERN.matcher(sql);
		if(!matcher.find()) {
			callback.call(sql, null);
			return;
		}

		Callback<Long> nextId = new Callback<Long>() {
			@Override
			public void call(final Long seq, Throwable cause) {
				if (seq == null) {
					callback.call(null, cause);
				} else {
					// 将"MATCHED_FEATURE+表名"替换成序列号。
					String executeSql = sql.replace(matcher.group(1), seq + "");
					callback.call(executeSql, null);
				}
			}
		};
		try {
			String tableName = matcher.group(2).toUpperCase();
			this.sequenceHandler.nextId(tableName, nextId);
		} catch (Throwable cause) {
			callback.call(null, cause);
		}
	}


    // Just for test
	public String getTableName(String sql) {
		Matcher matcher = SEQ_PATTERN.matcher(sql);
		if(matcher.find()) {
			return  matcher.group(2);
		}

		return null;
	}

}
