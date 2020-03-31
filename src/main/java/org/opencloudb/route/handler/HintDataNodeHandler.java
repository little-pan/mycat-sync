package org.opencloudb.route.handler;

import java.sql.SQLNonTransientException;

import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.cache.LayeredCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.ServerConnection;
import org.slf4j.*;

/**
 * 处理注释中类型为datanode 的情况
 * 
 * @author zhuam
 */
public class HintDataNodeHandler implements HintHandler {
	
	private static final Logger log = LoggerFactory.getLogger(HintSchemaHandler.class);

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType, String realSQL,
                                String charset, ServerConnection sc, LayeredCachePool cachePool, String hintSQLValue)
					throws SQLNonTransientException {
		
		String stmt = realSQL;
		log.debug("route datanode sql hint from '{}'", stmt);
		
		RouteResultset rrs = new RouteResultset(stmt, sqlType);
		MycatServer server = MycatServer.getContextServer();
		PhysicalDBNode dataNode = server.getConfig().getDataNodes().get(hintSQLValue);
		if (dataNode != null) {			
			rrs = RouterUtil.routeToSingleNode(rrs, dataNode.getName(), stmt);
		} else {
			String msg = "can't find hint datanode:" + hintSQLValue;
			log.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		
		return rrs;
	}

}
