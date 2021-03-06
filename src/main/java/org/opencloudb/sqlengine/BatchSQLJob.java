package org.opencloudb.sqlengine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.opencloudb.MycatServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchSQLJob {

	static final Logger log = LoggerFactory.getLogger(BatchSQLJob.class);

	private ConcurrentHashMap<Integer, SQLJob> runningJobs = new ConcurrentHashMap<>();
	private ConcurrentLinkedQueue<SQLJob> waitingJobs = new ConcurrentLinkedQueue<>();
	private volatile boolean noMoreJobInput = false;

	public void addJob(SQLJob newJob, boolean parallExecute) {
		if (parallExecute) {
			runJob(newJob);
		} else {
			waitingJobs.offer(newJob);
			if (runningJobs.isEmpty()) {
				SQLJob job = waitingJobs.poll();
				if (job != null) {
					runJob(job);
				}
			}
		}
	}

	public void setNoMoreJobInput(boolean noMoreJobInput) {
		this.noMoreJobInput = noMoreJobInput;
	}

	private void runJob(SQLJob newJob) {
		MycatServer server = MycatServer.getContextServer();
		runningJobs.put(newJob.getId(), newJob);
		server.getProcessorPool().getNextProcessor().execute(newJob);
	}

	public boolean jobFinished(SQLJob sqlJob) {
		log.debug("{}: job finished ", sqlJob);

		runningJobs.remove(sqlJob.getId());
		SQLJob job = waitingJobs.poll();
		if (job != null) {
			runJob(job);
			return false;
		} else {
			if (noMoreJobInput) {
				return runningJobs.isEmpty() && waitingJobs.isEmpty();
			} else {
				return false;
			}
		}

	}
}
