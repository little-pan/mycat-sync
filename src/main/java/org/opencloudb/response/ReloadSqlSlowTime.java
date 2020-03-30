package org.opencloudb.response;

import java.util.Map;

import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.stat.UserStat;
import org.opencloudb.stat.UserStatAnalyzer;
import org.slf4j.*;

public class ReloadSqlSlowTime {

	private static final Logger log = LoggerFactory.getLogger(ReloadSqlSlowTime.class);

    public static void execute(ManagerConnection c,long time) {
    	
    	Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
        	userStat.setSlowTime(time);
        }
    	
        StringBuilder s = new StringBuilder();
        s.append(c).append("Reset show  @@sql.slow="+time+" time success by manager");
        log.warn(s.toString());
        
        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = "Reset show  @@sql.slow time success".getBytes();
        ok.write(c);
        System.out.println(s.toString());
    }

}