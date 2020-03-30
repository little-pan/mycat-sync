package org.opencloudb.response;

import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.stat.QueryConditionAnalyzer;
import org.slf4j.*;

public class ReloadQueryCf {
	
	private static final Logger log = LoggerFactory.getLogger(ReloadSqlSlowTime.class);

    public static void execute(ManagerConnection c, String cf) {
    	if (cf == null ) {
    	    cf = "NULL";
        }
    	
    	QueryConditionAnalyzer.getInstance().setCf(cf);
        log.info("Reset show @@sql.condition={} success by manager in frontend {}", cf, c);
        
        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 0;
        ok.serverStatus = 2;
        ok.message = "Reset show  @@sql.condition success".getBytes();
        ok.write(c);
    }

}
