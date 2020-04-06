package org.opencloudb.mysql;

import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.net.BackendConnection;
import org.opencloudb.mpp.LoadData;
import org.opencloudb.net.NioProcessor;
import org.opencloudb.net.ResourceTask;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.util.Callback;
import org.opencloudb.util.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by nange on 2015/3/31.
 */
public class LoadDataUtil {

    static final String PROP_YIELD_SIZE = "org.opencloudb.mysql.loadData.yieldSize";
    static final int YIELD_SIZE = Integer.getInteger(PROP_YIELD_SIZE, 16 << 20);

    public static void requestFileDataResponse(byte[] data, BackendConnection conn) {
        byte packId = data[3];
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        LoadData loadData = rrn.getLoadData();

        writeToBackConnection(packId, loadData, conn);
    }

    static void writeToBackConnection(byte packID, LoadData loadData, BackendConnection conn) {
        InputStream in = null;
        int packSize;
        LoadDataTask task;

        boolean failed = true;
        try {
            File dataFile = new File(loadData.getFileName());
            in = IoUtil.fileInputStream(dataFile);

            MycatServer server = MycatServer.getContextServer();
            SystemConfig conf = server.getConfig().getSystem();
            packSize = conf.getProcessorBufferChunk() - 5;

            task = new LoadDataTask(in, packID, packSize, conn);
            failed = false;
        } catch (IOException e) {
            throw new RuntimeException("File open failed", e);
        } finally {
            if (failed) {
                IoUtil.close(in);
                // file open failed, but server also needs one packet
                sendEmptyPacket(conn, packID);
            }
        }

        task.run();
    }

    static void sendEmptyPacket(BackendConnection conn, byte packID) {
        byte[] empty = new byte[]{0, 0, 0, 3};
        empty[3] = ++packID;
        conn.write(empty);
    }

    static class LoadDataTask implements ResourceTask, Callback<Boolean> {

        static final Logger log = LoggerFactory.getLogger(LoadDataTask.class);

        final InputStream in;
        private byte packID;
        final int packSize;
        final byte[] buffer;

        final BackendConnection conn;

        public LoadDataTask(InputStream in, byte packID, int packSize, BackendConnection conn) {
            this.in = in;
            this.packID = packID;
            this.packSize = packSize;
            this.conn = conn;
            this.buffer = new byte[this.packSize];
        }

        @Override
        public void release(String reason) {
            IoUtil.close(this.in);
            log.debug("Close the input stream: {}", reason);
        }

        @Override
        public void run() {
            boolean completed = false;
            boolean failed = true;

            try {
                int len, size = 0;

                while ((len = this.in.read(this.buffer)) != -1) {
                    byte[] temp;

                    size += len;
                    if (len == this.packSize) {
                        temp = this.buffer;
                    } else {
                        temp = new byte[len];
                        System.arraycopy(this.buffer, 0, temp, 0, len);
                    }
                    BinaryPacket packet = new BinaryPacket();
                    packet.packetId = ++this.packID;
                    packet.data = temp;
                    packet.write(this.conn);

                    if (size >= YIELD_SIZE) {
                        log.debug("yield when size reaches {} in this round", size);
                        // Note: load later for easing buffer accumulation in write queue(GC overhead issue),
                        // and doesn't block processor
                        this.conn.writeCompleteCallback(this);
                        failed = false;
                        return;
                    }
                }

                completed = true;
                failed = false;
            } catch (IOException e) {
                throw new RuntimeException("'load data' send to backend error", e);
            } finally {
                if (failed || completed) {
                    try {
                        // even if failed, but server also needs one packet
                        sendEmptyPacket(this.conn, this.packID);
                    } finally {
                        String s = "'load data' completed";
                        if (failed) {
                            s = "'load data' send to backend failed";
                        }
                        release(s);
                    }
                }
            }
        }

        @Override
        public void call(Boolean result, Throwable cause) {
            if (cause != null) {
                release("Backend write error: " + cause);
                return;
            }

            NioProcessor processor = NioProcessor.ensureRunInProcessor();
            processor.executeLater(this);
        }

    }

}
