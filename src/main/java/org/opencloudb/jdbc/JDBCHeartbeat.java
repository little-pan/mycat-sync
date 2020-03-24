package org.opencloudb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.statistic.HeartbeatRecorder;
import org.slf4j.*;

public class JDBCHeartbeat extends DBHeartbeat {

	private static final Logger log = LoggerFactory.getLogger(JDBCHeartbeat.class);

	private final ReentrantLock lock;
	private final JDBCDataSource source;
    private final JDBCHeartbeatCaller caller;

    private Long lastSendTime = System.currentTimeMillis();
    private Long lastRecvTime = System.currentTimeMillis();
    
	public JDBCHeartbeat(JDBCDataSource source) {
		this.source = source;
		this.lock = new ReentrantLock(false);
		this.status = INIT_STATUS;
		this.heartbeatSQL = source.getHostConfig().getHeartbeatSQL().trim();
		this.caller = new JDBCHeartbeatCaller(this.heartbeatSQL);
	}

	@Override
	public void start() {
		if (this.caller.isUseJdbc4Validation()) {
			String host = this.source.getConfig().getHostName();
			log.debug("use jdbc4 validation for no sql configured in host '{}'", host);
		}

		lock.lock();
		try {
			this.isStop.compareAndSet(true, false);
			this.status = DBHeartbeat.OK_STATUS;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void stop() {
		lock.lock();
		try {
			if (this.isStop.compareAndSet(false, true)) {
				this.isChecking.set(false);
				String host = this.source.getConfig().getHostName();
				log.info("Heartbeat stopped in host '{}'", host);
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public String getLastActiveTime() {
	    long t = lastRecvTime;
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(t));
	}

	@Override
	public long getTimeout()
	{
		return 0;
	}

	@Override
	public HeartbeatRecorder getRecorder() {
        this.recorder.set(lastRecvTime - lastSendTime);
        return recorder;
    }
	
	@Override
	public void heartbeat() {
		if (isStop()) {
            return;
        }

        this.lastSendTime = System.currentTimeMillis();
        this.lock.lock();
		try {
			if (log.isDebugEnabled()) {
				log.debug("JDBCHeartBeat: url '{}', sql '{}'", getUrl(), this.heartbeatSQL);
			}
            this.isChecking.set(true);

			final boolean valid;
			try (Connection c = this.source.getConnection()) {
				valid = this.caller.call(c, getHeartbeatTimeout());
			}
			if (valid) {
				this.status = OK_STATUS;
				log.debug("JDBCHeartBeat: ok");
			} else {
				this.status = ERROR_STATUS;
				log.error("JDBCHeartBeat error: invalid connection for url '{}", getUrl());
			}
		} catch (SQLException e) {
		    log.error("JDBCHeartBeat error: url '" + getUrl() + "'", e);
            this.status = ERROR_STATUS;
		} finally {
			this.isChecking.set(false);
            this.lock.unlock();
            this.lastRecvTime = System.currentTimeMillis();
		}
	}

	private String getUrl () {
	    return this.source.getConfig().getUrl();
    }

}
