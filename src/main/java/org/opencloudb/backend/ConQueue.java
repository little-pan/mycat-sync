package org.opencloudb.backend;

import org.opencloudb.net.BackendConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConQueue {

	private final Queue<BackendConnection> autoCommitCons = new ConcurrentLinkedQueue<>();
	private final Queue<BackendConnection> manCommitCons = new ConcurrentLinkedQueue<>();
	private long executeCount;

	public BackendConnection takeIdleCon(boolean autoCommit) {
		Queue<BackendConnection> q1 = this.autoCommitCons;
		Queue<BackendConnection> q2 = this.manCommitCons;

		if (!autoCommit) {
			q1 = this.manCommitCons;
			q2 = this.autoCommitCons;
		}
		BackendConnection con = q1.poll();
		if (con != null && !con.isClosedOrQuit()) {
			return con;
		}

		con = q2.poll();
		if (con != null && !con.isClosedOrQuit()) {
			return con;
		}

		return null;
	}

	public long getExecuteCount() {
		return executeCount;
	}

	public void incExecuteCount() {
		this.executeCount++;
	}

	public void removeCon(BackendConnection con) {
		if (!this.autoCommitCons.remove(con)) {
			this.manCommitCons.remove(con);
		}
	}

	public boolean isPooled(BackendConnection con) {
		return  (this.autoCommitCons.contains(con) || this.manCommitCons.contains(con));
	}

	public Queue<BackendConnection> getAutoCommitCons() {
		return this.autoCommitCons;
	}

	public Queue<BackendConnection> getManCommitCons() {
		return this.manCommitCons;
	}

	public List<BackendConnection> getIdleConsToClose(int count) {
		List<BackendConnection> readyCloseCons = new ArrayList<>(count);
		while (!manCommitCons.isEmpty() && readyCloseCons.size() < count) {
			BackendConnection theCon = manCommitCons.poll();
			if (theCon != null) {
				readyCloseCons.add(theCon);
			}
		}
		while (!autoCommitCons.isEmpty() && readyCloseCons.size() < count) {
			BackendConnection theCon = autoCommitCons.poll();
			if (theCon != null) {
				readyCloseCons.add(theCon);
			}

		}

		return readyCloseCons;
	}

}
