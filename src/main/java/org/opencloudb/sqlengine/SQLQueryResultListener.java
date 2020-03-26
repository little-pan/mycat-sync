package org.opencloudb.sqlengine;

public interface SQLQueryResultListener<T> {

	void onResult(T result);

}
