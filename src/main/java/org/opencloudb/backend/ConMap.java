package org.opencloudb.backend;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.opencloudb.MycatServer;
import org.opencloudb.net.ConnectionManager;
import org.opencloudb.net.BackendConnection;

public class ConMap {
	// key -schema
	private final ConcurrentHashMap<String, ConQueue> items = new ConcurrentHashMap<>();

	public ConQueue getSchemaConQueue(String schema) {
		ConQueue queue = this.items.get(schema);
		if (queue == null) {
			ConQueue newQueue = new ConQueue();
			queue = this.items.putIfAbsent(schema, newQueue);
			return (queue == null) ? newQueue : queue;
		}

		return queue;
	}

	public BackendConnection tryTakeCon(final String schema, boolean autoCommit) {
		final ConQueue queue = this.items.get(schema);
		BackendConnection con = tryTakeCon(queue, autoCommit);
		if (con != null) {
			return con;
		}

		for (ConQueue queue2 : this.items.values()) {
			if (queue != queue2) {
				con = tryTakeCon(queue2, autoCommit);
				if (con != null) {
					return con;
				}
			}
		}

		return null;
	}

	private BackendConnection tryTakeCon(ConQueue queue, boolean autoCommit) {
		if (queue != null) {
			return queue.takeIdleCon(autoCommit);
		} else {
			return null;
		}
	}

	public Collection<ConQueue> getAllConQueue() {
		return items.values();
	}

	public int getActiveCountForSchema(String schema, PhysicalDataSource dataSource) {
		int total = 0;

        for (BackendConnection con: getBackends().values()) {
        	String conSchema = con.getSchema();
        	PhysicalDataSource conDataSource = con.getPool();
			if (conSchema.equals(schema) && conDataSource == dataSource) {
				if (con.isBorrowed() && !con.isClosed()) {
					total++;
				}
			}
        }

        return total;
    }

	public int getActiveCountForDs(PhysicalDataSource dataSource) {
		int total = 0;

        for (BackendConnection con : getBackends().values()) {
			PhysicalDataSource conDataSource = con.getPool();
			if (conDataSource == dataSource) {
				if (con.isBorrowed() && !con.isClosed()) {
					total++;
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
			PhysicalDataSource conDataSource = con.getPool();
			if(conDataSource == dataSouce){
				con.close(reason);
				it.remove();
			}
        }

		this.items.clear();
	}

	protected ConcurrentMap<Long, BackendConnection> getBackends() {
	    ConnectionManager connectionManager = getConnectionManager();
        return connectionManager.getBackends();
    }

    protected ConnectionManager getConnectionManager() {
		MycatServer server = MycatServer.getContextServer();
	    return server.getConnectionManager();
    }

}
