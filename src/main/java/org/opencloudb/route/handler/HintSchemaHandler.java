package org.opencloudb.route.handler;

import java.sql.SQLNonTransientException;

import org.opencloudb.MycatServer;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteStrategy;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.server.ServerConnection;
import org.slf4j.*;

/**
 * 处理注释中类型为schema 的情况（按照指定schema做路由解析）
 */
public class HintSchemaHandler implements HintHandler {

	private static final Logger log = LoggerFactory.getLogger(HintSchemaHandler.class);

	private RouteStrategy routeStrategy;
    
    public HintSchemaHandler() {
		this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
	}
	/**
	 * 从全局的schema列表中查询指定的schema是否存在， 如果存在则替换connection属性中原有的schema，
	 * 如果不存在，则throws SQLNonTransientException，表示指定的schema 不存在
	 * 
	 * @param sysConfig
	 * @param schema
	 * @param sqlType
	 * @param realSQL
	 * @param charset
	 * @param sc
	 * @param cachePool
	 * @param hintSQLValue
	 * @return
	 * @throws SQLNonTransientException
	 */
	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset, ServerConnection sc,
			LayerCachePool cachePool, String hintSQLValue)
			throws SQLNonTransientException {
		MycatServer server = MycatServer.getContextServer();
	    SchemaConfig tempSchema = server.getConfig().getSchemas().get(hintSQLValue);
		if (tempSchema != null) {
			return routeStrategy.route(sysConfig, tempSchema, sqlType, realSQL, charset, sc, cachePool);
		} else {
			String msg = "Can't find hint schema:" + hintSQLValue;
			log.warn(msg);
			throw new SQLNonTransientException(msg);
		}
	}
}
