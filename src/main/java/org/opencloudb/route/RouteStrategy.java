package org.opencloudb.route;

import java.sql.SQLNonTransientException;

import org.opencloudb.cache.LayeredCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.server.ServerConnection;

/**
 * 路由策略接口
 * @author wang.dw
 *
 */
public interface RouteStrategy {

	RouteResultset route(SystemConfig sysConfig,
			SchemaConfig schema,int sqlType, String originSQL,
						 String charset, ServerConnection sc, LayeredCachePool cachePool)
			throws SQLNonTransientException;

}
