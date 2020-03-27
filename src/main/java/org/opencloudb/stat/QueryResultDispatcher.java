package org.opencloudb.stat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opencloudb.net.NioProcessor;
import org.slf4j.*;

/**
 * SQL执行后的派发  QueryResult 事件
 * 
 * @author zhuam
 *
 */
public class QueryResultDispatcher {
	
	// 是否派发QueryResult 事件
	private final static AtomicBoolean isClosed = new AtomicBoolean(false);
	
	private static final Logger log = LoggerFactory.getLogger(QueryResultDispatcher.class);
	
	private static List<QueryResultListener> listeners = new CopyOnWriteArrayList<>();

	// 初始化强制加载
	static {
		listeners.add(UserStatAnalyzer.getInstance());
		listeners.add(TableStatAnalyzer.getInstance());
		listeners.add(QueryConditionAnalyzer.getInstance());
	}
	
	public static boolean close() {
		if (isClosed.compareAndSet(false, true)) {
			return true;
		}
		return false;
	}
	
	public static boolean open() {
		if (isClosed.compareAndSet(true, false)) {
			return true;
		}
		return false;
	}
	
	public static void addListener(QueryResultListener listener) {
		if (listener == null) {
			throw new NullPointerException();
		}
		listeners.add(listener);
	}

	public static void removeListener(QueryResultListener listener) {
		listeners.remove(listener);
	}

	public static void removeAllListener() {
		listeners.clear();
	}
	
	public static void dispatchQuery(final QueryResult queryResult) {
		if (isClosed.get() ) {
			return;
		}

		NioProcessor processor = NioProcessor.currentProcessor();
		processor.execute(new Runnable() {
			@Override
			public void run() {
				for(QueryResultListener listener: listeners) {
					listener.onQueryResult(queryResult);
				}					
			}
		});
	}

}