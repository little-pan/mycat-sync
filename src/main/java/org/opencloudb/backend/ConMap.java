package org.opencloudb.backend;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.opencloudb.MycatServer;
import org.opencloudb.jdbc.JDBCConnection;
import org.opencloudb.net.ConnectionManager;

public class ConMap {
	// key -schema
	private final ConcurrentHashMap<String, ConQueue> items = new ConcurrentHashMap<String, ConQueue>();

	public ConQueue getSchemaConQueue(String schema) {
		ConQueue queue = items.get(schema);
		if (queue == null) {
			ConQueue newQueue = new ConQueue();
			queue = items.putIfAbsent(schema, newQueue);
			return (queue == null) ? newQueue : queue;
		}
		return queue;
	}

	public BackendConnection tryTakeCon(final String schema, boolean autoCommit) {
		final ConQueue queue = items.get(schema);
		BackendConnection con = tryTakeCon(queue, autoCommit);
		if (con != null) {
			return con;
		} else {
			for (ConQueue queue2 : items.values()) {
				if (queue != queue2) {
					con = tryTakeCon(queue2, autoCommit);
					if (con != null) {
						return con;
					}
				}
			}
		}
		return null;

	}

	private BackendConnection tryTakeCon(ConQueue queue, boolean autoCommit) {

		BackendConnection con = null;
		if (queue != null && ((con = queue.takeIdleCon(autoCommit)) != null)) {
			return con;
		} else {
			return null;
		}

	}

	public Collection<ConQueue> getAllConQueue() {
		return items.values();
	}

	public int getActiveCountForSchema(String schema, PhysicalDataSource dataSouce) {
		int total = 0;

        for (BackendConnection con : getBackends().values()) {
            if (con instanceof JDBCConnection) {
                JDBCConnection jdbcCon = (JDBCConnection) con;
                if (jdbcCon.getSchema().equals(schema) && jdbcCon.getPool() == dataSouce) {
                    if (jdbcCon.isBorrowed()) {
                        total++;
                    }
                }
            }
        }

        return total;
    }

	public int getActiveCountForDs(PhysicalDataSource dataSouce) {
		int total = 0;

        for (BackendConnection con : getBackends().values()) {
            if (con instanceof JDBCConnection) {
                JDBCConnection jdbcCon = (JDBCConnection) con;
                if (jdbcCon.getPool() == dataSouce) {
                    if (jdbcCon.isBorrowed() && !jdbcCon.isClosed()) {
                        total++;
                    }
                }
            }
        }

        return total;
    }

    public void clearConnections(String reason, PhysicalDataSource dataSouce) {
        ConcurrentMap<Long, BackendConnection> map = getBackends();
        Iterator<Entry<Long, BackendConnection>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, BackendConnection> entry = it.next();
            BackendConnection con = entry.getValue();
            if(con instanceof JDBCConnection){
                if(((JDBCConnection) con).getPool() == dataSouce){
                    con.close(reason);
                    it.remove();
                }
            }
        }

		this.items.clear();
	}

	protected ConcurrentMap<Long, BackendConnection> getBackends() {
	    ConnectionManager connectionManager = getConnectionManager();
        return connectionManager.getBackends();
    }

    protected ConnectionManager getConnectionManager() {
	    return MycatServer.getInstance().getConnectionManager();
    }

}
