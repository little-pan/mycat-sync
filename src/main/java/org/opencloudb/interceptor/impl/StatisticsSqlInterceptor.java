package org.opencloudb.interceptor.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.interceptor.SQLInterceptor;
import org.opencloudb.server.parser.ServerParse;
import org.slf4j.*;

import java.io.File;

public class StatisticsSqlInterceptor implements SQLInterceptor {
    
    private final class StatisticsSqlRunner implements Runnable {
        
        private int    sqltype = 0;
        private String sqls    = "";
        
        public StatisticsSqlRunner(int sqltype, String sqls) {
            this.sqltype = sqltype;
            this.sqls = sqls;
        }
        
        public void run() {
            try {
                MycatServer server = MycatServer.getContextServer();
                SystemConfig sysconfig = server.getConfig().getSystem();
                String sqlInterceptorType = sysconfig.getSqlInterceptorType();
                String sqlInterceptorFile = sysconfig.getSqlInterceptorFile();
                
                String[] sqlInterceptorTypes = sqlInterceptorType.split(",");
                for (String type : sqlInterceptorTypes) {
                    if (StatisticsSqlInterceptor.parseType(type.toUpperCase()) == sqltype) {
                        switch (sqltype) {
                            case ServerParse.SELECT:
                                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, "SELECT:"
                                    + sqls + "");
                                break;
                            case ServerParse.UPDATE:
                                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, "UPDATE:"
                                    + sqls);
                                break;
                            case ServerParse.INSERT:
                                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, "INSERT:"
                                    + sqls);
                                break;
                            case ServerParse.DELETE:
                                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, "DELETE:"
                                    + sqls);
                                break;
                            default:
                                break;
                        }
                    }
                }
                
            } catch (Exception e) {
                log.error("interceptSQL error", e);
            }
        }
    }
    
    private static final Logger         log  = LoggerFactory.getLogger(StatisticsSqlInterceptor.class);
    
    private static Map<String, Integer> typeMap = new HashMap<>();
    static {
        typeMap.put("SELECT", 7);
        typeMap.put("UPDATE", 11);
        typeMap.put("INSERT", 4);
        typeMap.put("DELETE", 3);
    }
    
    public static int parseType(String type) {
        return typeMap.get(type);
    }
    
    /**
     * 方法追加文件：使用FileWriter
     */
    private static synchronized void appendFile(String fileName, String content) {
        
        Calendar calendar = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dayFile = dateFormat.format(calendar.getTime());
        
        try {
            String newFileName = fileName;
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            String[] title = newFileName.split("\\.");
            if (title.length == 2) {
                newFileName = title[0] + dayFile + "." + title[1];
            }
            File file = new File(newFileName);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter writer = new FileWriter(file, true);
            String newContent = content.replaceAll("[\\t\\n\\r]", "")
                + System.getProperty("line.separator");
            writer.write(newContent);
            
            writer.close();
        } catch (IOException e) {
            log.error("appendFile error", e);
        }
    }
    
    /**
     * interceptSQL ,
     * 	type :insert,delete,update,select
     *  exectime:xxx ms
     *  log content : select:select 1 from table,exectime:100ms,shared:1
     * etc
     */
    @Override
    public String interceptSQL(String sql, int sqlType) {
        log.debug("sql interceptSQL:");
        final String sqls = DefaultSqlInterceptor.processEscape(sql);
        MycatServer server = MycatServer.getContextServer();
        server.getBusinessExecutor().execute(new StatisticsSqlRunner(sqlType, sqls));
        return sql;
    }
    
}
