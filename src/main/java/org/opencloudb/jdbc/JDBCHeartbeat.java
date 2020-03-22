package org.opencloudb.jdbc;

import java.sql.Connection;
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
    private final boolean heartbeatnull;
    private Long lastSendTime = System.currentTimeMillis();
    private Long lastReciveTime = System.currentTimeMillis();
    
	public JDBCHeartbeat(JDBCDatasource source) {
		this.source = source;
		lock = new ReentrantLock(false);
		this.status = INIT_STATUS;
		this.heartbeatSQL = source.getHostConfig().getHearbeatSQL().trim();
		this.heartbeatnull= heartbeatSQL.length()==0;
	}

	@Override
	public void start() {
		if (this.heartbeatnull){
			stop();
			return;
		}
		lock.lock();
		try {
			isStop.compareAndSet(true, false);
			this.status = DBHeartbeat.OK_STATUS;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void stop() {
		lock.lock();
		try {
			if (isStop.compareAndSet(false, true)) {
				isChecking.set(false);
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public String getLastActiveTime() {
	    long t = lastReciveTime;
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
        recorder.set(lastReciveTime - lastSendTime);
        return recorder;
    }
	
	@Override
	public void heartbeat() {
	    
		if (this.isStop.get()) {
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
			try (Connection c = this.source.getConnection()) {
				try (Statement s = c.createStatement()) {
					s.execute(sql);
				}
			}
            this.status = OK_STATUS;
            log.debug("JDBCHeartBeat: ok");
		} catch (Exception ex) {
		    log.error("JDBCHeartBeat error: url '" + getUrl() + "'", ex);
            this.status = ERROR_STATUS;
		} finally {
            this.lock.unlock();
			this.isChecking.set(false);
            this.lastReciveTime = System.currentTimeMillis();
		}
	}

	private String getUrl () {
	    return this.source.getConfig().getUrl();
    }

}
