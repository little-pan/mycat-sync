package org.opencloudb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.statistic.HeartbeatRecorder;
import org.slf4j.*;

public class JDBCHeartbeat extends DBHeartbeat {
	private static final Logger log = LoggerFactory.getLogger(JDBCHeartbeat.class);

	private final ReentrantLock lock;
	private final JDBCDatasource source;
    private final boolean noSqlGiven;
    private Long lastSendTime = System.currentTimeMillis();
    private Long lastRecvTime = System.currentTimeMillis();
    
	public JDBCHeartbeat(JDBCDatasource source) {
		this.source = source;
		this.lock = new ReentrantLock(false);
		this.status = INIT_STATUS;
		this.heartbeatSQL = source.getHostConfig().getHeartbeatSQL().trim();
		this.noSqlGiven = heartbeatSQL.length() == 0;
	}

	@Override
	public void start() {
		if (this.noSqlGiven) {
			String host = this.source.getConfig().getHostName();
			log.warn("Heartbeat stops for no sql configured in host '{}'", host);
			stop();
			return;
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
            this.isChecking.set(true);
			String sql = this.heartbeatSQL;
			if (log.isDebugEnabled()) {
                log.debug("JDBCHeartBeat: url '{}', sql '{}'", getUrl(), sql);
            }
			try (Connection c = this.source.getConnection();
				 Statement s = c.createStatement()) {
				s.execute(sql);
			}
            this.status = OK_STATUS;
            log.debug("JDBCHeartBeat: ok");
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
