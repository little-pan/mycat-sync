package org.opencloudb.mysql;

import org.opencloudb.MycatServer;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.mpp.LoadData;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.util.IoUtil;

import java.io.*;

/**
 * Created by nange on 2015/3/31.
 */
public class LoadDataUtil {

    public static void requestFileDataResponse(byte[] data, BackendConnection conn) {
        byte packId = data[3];
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        LoadData loadData = rrn.getLoadData();

        writeToBackConnection(packId, loadData, conn);
    }

    static void writeToBackConnection(byte packID, LoadData loadData, BackendConnection conn) {
        InputStream in = null;
        try {
            File dataFile = new File(loadData.getFileName());
            in = IoUtil.fileInputStream(dataFile);

            MycatServer server = MycatServer.getContextServer();
            int packSize = server.getConfig().getSystem().getProcessorBufferChunk() - 5;
            // int packSize = backendAIOConnection.getMaxPacketSize() / 32;
            // int packSize = 65530;
            byte[] buffer = new byte[packSize];
            int len;

            while ((len = in.read(buffer)) != -1) {
                byte[] temp;
                if (len == packSize) {
                    temp = buffer;
                } else {
                    temp = new byte[len];
                    System.arraycopy(buffer, 0, temp, 0, len);
                }
                BinaryPacket packet = new BinaryPacket();
                packet.packetId = ++packID;
                packet.data = temp;
                packet.write(conn);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IoUtil.close(in);
            // file open failed, but server also needs one packet
            byte[] empty = new byte[]{0, 0, 0, 3};
            empty[3] = ++packID;
            conn.write(empty);
        }
    }

}
