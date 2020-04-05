package org.opencloudb.mysql;

import org.opencloudb.MycatServer;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.mpp.LoadData;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.util.IoUtil;

import java.io.*;
import java.util.List;

/**
 * Created by nange on 2015/3/31.
 */
public class LoadDataUtil {

    public static void requestFileDataResponse(byte[] data, BackendConnection conn) {
        byte packId = data[3];
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        LoadData loadData = rrn.getLoadData();
        List<String> valuesList = loadData.getData();

        try {
            if(valuesList != null && valuesList.size() > 0) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                int n = valuesList.size();
                for (int i = 0; i < n; i++) {
                    String values = valuesList.get(i);
                    String s =(i == n-1)? values: values + loadData.getLineTerminatedBy();
                    byte[] bytes = s.getBytes(loadData.getCharset());
                    bos.write(bytes);
                }
                InputStream in = new ByteArrayInputStream(bos.toByteArray());
                packId = writeToBackConnection(packId, in, conn);
            } else {
                // 从文件读取
                File dataFile = new File(loadData.getFileName());
                InputStream in = new FileInputStream(dataFile);
                boolean failed = true;
                try {
                    BufferedInputStream bin = new BufferedInputStream(in);
                    packId = writeToBackConnection(packId, bin, conn);
                    failed = false;
                } finally {
                    if (failed) {
                        IoUtil.close(in);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            // file open failed, but server also needs one packet
            byte[] empty = new byte[]{0, 0, 0, 3};
            empty[3] = ++packId;
            conn.write(empty);
        }
    }

    public static byte writeToBackConnection(byte packID, InputStream in, BackendConnection conn)
            throws IOException {
        try {
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

        } finally {
            IoUtil.close(in);
        }

        return  packID;
    }

}
