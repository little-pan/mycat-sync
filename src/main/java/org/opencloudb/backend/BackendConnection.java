package org.opencloudb.backend;

import java.io.IOException;

import org.opencloudb.mysql.handler.ResponseHandler;
import org.opencloudb.net.ClosableConnection;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;

public interface BackendConnection extends ClosableConnection {

	boolean isModifiedSQLExecuted();

	boolean isFromSlaveDB();

	String getSchema();

	void setSchema(String newSchema);

	long getLastTime();

	boolean isClosedOrQuit();

	void setAttachment(Object attachment);

	void quit();

	void setLastTime(long currentTimeMillis);

	void release();

	boolean setResponseHandler(ResponseHandler commandHandler);

	void commit() throws BackendException;

 	void query(String sql) throws UnsupportedOperationException;

	Object getAttachment();

	void execute(RouteResultsetNode node, ServerConnection source, boolean autocommit)
			throws IOException;

	void recordSql(String host, String schema, String statement);

    boolean syncAndExcute();

    void rollback() throws BackendException;

    boolean isBorrowed();

    void setBorrowed(boolean borrowed);

    int getTxIsolation();

    boolean isAutocommit();

    long getId();

}
