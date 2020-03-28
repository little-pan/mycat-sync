package org.opencloudb.sqlcmd;

import org.opencloudb.net.BackendConnection;
import org.opencloudb.server.ServerSession;

/**
 * SQL control command like 'set XXX, commit etc', only return OK /Err packet,
 * can't return result set.
 * 
 * @author wuzhih
 * 
 */
public interface SQLCtrlCommand {

	boolean releaseConOnErr();
	
	boolean releaseConOnOK();
	
	void sendCommand(ServerSession session, BackendConnection con);

	/**
	 * 收到错误数据包的响应处理
	 */
	void errorResponse(ServerSession session, byte[] err, int total, int failed);

	/**
	 * 收到OK数据包的响应处理
	 */
	void okResponse(ServerSession session, byte[] ok);

}
