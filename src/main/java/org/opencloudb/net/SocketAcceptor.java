package org.opencloudb.net;

public interface SocketAcceptor {

	boolean DAEMON = Boolean.getBoolean("org.opencloudb.server.daemon");

	void start();

	String getName();

	int getPort();

}
