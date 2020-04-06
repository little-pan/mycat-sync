/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.server.handler;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.opencloudb.MycatServer;
import org.opencloudb.cache.LayeredCachePool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.mpp.LoadData;
import org.opencloudb.net.FrontendException;
import org.opencloudb.net.handler.LoadDataInfileHandler;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.net.mysql.RequestFilePacket;
import org.opencloudb.parser.druid.DruidShardingParseInfo;
import org.opencloudb.parser.druid.MycatStatementParser;
import org.opencloudb.parser.druid.RouteCalculateUnit;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.ServerSession;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.IoUtil;
import org.opencloudb.util.ObjectUtil;
import org.opencloudb.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLNonTransientException;
import java.util.*;

/** <p>
 * mysql命令行客户端也需要启用local file权限，加参数--local-infile=1：
 * 1）JDBC则正常，不用设置；
 * 2）'load data' sql中的CHARACTER SET 'gbk'，其中的字符集必须引号括起来，否则druid解析出错
 * </p>
 *
 * <p>
 * All data writen to disk for memory issue and simplicity. Here only sequential read/write,
 * so it's very high efficient already, no need to be buffered in memory.
 *
 * @since 2020-04-06
 * @author little-pan
 * </p>
 */
public final class ServerLoadDataInfileHandler implements LoadDataInfileHandler {

    static final Logger log = LoggerFactory.getLogger(ServerLoadDataInfileHandler.class);

    private final ServerConnection serverConnection;

    private String sql;
    private String fileName;
    private byte packID;
    private MySqlLoadDataInFileStatement statement;

    private Map<String, LoadData> routeResultMap = new HashMap<>();
    private RouteResultset routeResultset;
    private boolean routeComplete;

    private Map<String, OutputStream> nodeOutFiles = new HashMap<>();
    private LoadData loadData;
    private String tempPath;
    private String tempFile;
    private OutputStream tempOutFile;

    private String tableName;
    private TableConfig tableConfig;
    private int partitionColumnIndex = -1;
    private LayeredCachePool tableId2DataNodeCache;
    private SchemaConfig schema;
    private boolean isStartLoadData;

    public int getPackID()
    {
        return packID;
    }

    public void setPackID(byte packID)
    {
        this.packID = packID;
    }

    public ServerLoadDataInfileHandler(ServerConnection serverConnection) {
        this.serverConnection = serverConnection;
    }

    private static String parseFileName(String fileName) {
        if (fileName.charAt(0) == '\'' || fileName.charAt(0) == '"') {
            return fileName.substring(1, fileName.length() - 1);
        }

        return fileName;
    }

    private void parseLoadDataParam() {
        this.loadData = new LoadData();
        SQLTextLiteralExpr rawLineEnd = (SQLTextLiteralExpr) statement.getLinesTerminatedBy();
        String lineTerminatedBy = rawLineEnd == null ? "\n" : rawLineEnd.getText();
        this.loadData.setLineTerminatedBy(lineTerminatedBy);

        SQLTextLiteralExpr rawFieldEnd = (SQLTextLiteralExpr) statement.getColumnsTerminatedBy();
        String fieldTerminatedBy = rawFieldEnd == null ? "\t" : rawFieldEnd.getText();
        this.loadData.setFieldTerminatedBy(fieldTerminatedBy);

        SQLTextLiteralExpr rawEnclosed = (SQLTextLiteralExpr) statement.getColumnsEnclosedBy();
        String enclose = rawEnclosed == null ? null : rawEnclosed.getText();
        this.loadData.setEnclose(enclose);

        SQLTextLiteralExpr escapeExpr =  (SQLTextLiteralExpr)statement.getColumnsEscaped() ;
         String escape = escapeExpr == null? "\\": escapeExpr.getText();
        this.loadData.setEscape(escape);

        String charset = this.statement.getCharset();
        if (charset == null) {
            charset = this.serverConnection.getCharset();
        }
        this.loadData.setCharset(charset);
        this.loadData.setFileName(this.fileName);

        log.debug("'load data' params: {}", this.loadData);
    }

    @Override
    public void start(String sql) {
        SQLStatementParser parser;

        clear();
        this.sql = sql;

        parser = new MycatStatementParser(sql);
        this.statement = (MySqlLoadDataInFileStatement) parser.parseStatement();
        this.fileName = parseFileName(this.statement.getFileName().toString());

        log.debug("'load data' sql: {}", this.statement);
        if (this.fileName == null) {
            String s = "File name is null!";
            this.serverConnection.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, s);
            clear();
            return;
        }

        MycatServer server = MycatServer.getContextServer();
        String schemaName = this.serverConnection.getSchema();
        this.schema = server.getConfig().getSchemas().get(schemaName);
        this.tableId2DataNodeCache = (LayeredCachePool) server.getCacheService()
                .getCachePool("TableID2DataNodeCache");
        this.tableName = this.statement.getTableName().getSimpleName().toUpperCase();
        this.tableConfig = this.schema.getTables().get(this.tableName);
        // tmp: $MYCAT_HOME/temp/SOURCE-ID/
        this.tempPath = SystemConfig.getHomePath() + File.separator + "temp"
                + File.separator + this.serverConnection.getId() + File.separator;
        this.tempFile = this.tempPath + "clientTemp.txt";

        List<SQLExpr> columns = this.statement.getColumns();
        if(this.tableConfig != null) {
            String pColumn = getPartitionColumn();
            if (pColumn != null && columns != null && columns.size() > 0) {
                for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++) {
                    String column = StringUtil.removeBackquote(columns.get(i).toString());
                    if (pColumn.equalsIgnoreCase(column)) {
                        this.partitionColumnIndex = i;
                        break;
                    }
                }
            }
        }

        parseLoadDataParam();
        routeByTable();

        String charset = this.loadData.getCharset();
        if (this.statement.isLocal()) {
            this.isStartLoadData = true;
            log.debug("request client's file '{}'", this.fileName);
            RequestFilePacket filePacket = new RequestFilePacket();
            Charset cs = Charset.forName(charset);
            filePacket.fileName = this.fileName.getBytes(cs);
            filePacket.packetId = 1;
            ByteBuffer buffer = this.serverConnection.allocate();
            filePacket.write(buffer, this.serverConnection, true);
            return;
        }

        // Check file
        final File infile = new File(this.fileName);
        if (!infile.isFile()) {
            String s = "'" + this.fileName + "' is not found!";
            this.serverConnection.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, s);
            clear();
            return;
        }
        if (!infile.canRead()) {
            String s = "'" + this.fileName + "' can't read!";
            this.serverConnection.writeErrMessage(ErrorCode.ER_ERROR_ON_READ, s);
            clear();
            return;
        }

        // Do load
        String lineSeq = this.loadData.getLineTerminatedBy();
        if (!this.routeComplete) {
            parseFileByLine(this.fileName, charset, lineSeq);
        }
        flushNodeFiles();
        RouteResultset rrs = buildResultSet(this.routeResultMap);
        ServerSession session = this.serverConnection.getSession();
        this.isStartLoadData = false;
        session.execute(rrs, ServerParse.LOAD_DATA_INFILE_SQL);
    }

    @Override
    public void handle(byte[] data) {
        BinaryPacket packet;

        try {
            if (this.sql == null) {
                String s = "Unknown command";
                this.serverConnection.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, s);
                clear();
                return;
            }

            ByteArrayInputStream in = new ByteArrayInputStream(data);
            packet = new BinaryPacket();
            packet.read(in);
        } catch (IOException e) {
            throw new FrontendException("'load data infile' data error", e);
        }

        appendToTempFile(packet.data);
    }

    private void appendToTempFile(byte[] data) {
        boolean failed = true;
        try {
            if (this.tempOutFile == null) {
                File file = new File(this.tempFile);
                this.tempOutFile = IoUtil.fileOutputStream(file, true);
            }
            this.tempOutFile.write(data);
            failed = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (failed) {
                flushTempFile();
            }
        }
    }

    private void flushTempFile() {
        IoUtil.close(this.tempOutFile);
        this.tempOutFile = null;
    }

    private RouteResultset routeByShardColumn(String sql, String[] values, int rowIndex) {
        final int sqlType = ServerParse.INSERT;
        RouteResultset rrs = new RouteResultset(sql, sqlType);
        rrs.setLoadData(true);

        // Route to specified dataNodes for shard table by partition rule
        if (this.partitionColumnIndex == -1 || this.partitionColumnIndex >= values.length) {
            String s = this.serverConnection.getSchema();
            s = String.format("No partition column in table '%s'.'%s'", s, this.tableName);
            SQLNonTransientException cause = new SQLNonTransientException(s);
            throw new FrontendException("Route failed", cause);
        }
        DruidShardingParseInfo ctx = new DruidShardingParseInfo();
        ctx.addTable(this.tableName);
        String value = values[this.partitionColumnIndex];
        String column = getPartitionColumn();
        RouteCalculateUnit calcUnit = new RouteCalculateUnit();
        value = parseFieldString(value, this.loadData.getEnclose());
        calcUnit.addShardingExpr(this.tableName, column, value);
        ctx.addRouteCalculateUnit(calcUnit);
        try {
            final Set<RouteResultsetNode> nodes = new TreeSet<>();
            for(RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
                RouteResultset result = RouterUtil.tryRouteForTables(this.schema, ctx, unit, rrs,
                        false, this.tableId2DataNodeCache);
                if(result != null && !result.isEmpty()) {
                    nodes.addAll(Arrays.asList(result.getNodes()));
                }
            }
            if (nodes.size() == 0) {
                String s = String.format("No route result for row %d col '%s' '%s' in file '%s'",
                        rowIndex, column, value, this.fileName);
                SQLNonTransientException cause = new SQLNonTransientException(s);
                throw new FrontendException("Route failed", cause);
            }
            RouteResultsetNode[] nodeList = nodes.toArray(new RouteResultsetNode[0]);
            rrs.setNodes(nodeList);

            return rrs;
        } catch (SQLNonTransientException e) {
            throw new FrontendException("Route failed", e);
        }
    }

    private RouteResultset routeByTable() throws FrontendException {
        final int sqlType = ServerParse.INSERT;
        RouteResultset rrs = new RouteResultset(this.sql, sqlType);
        rrs.setLoadData(true);

        if (this.tableConfig == null) {
            // Case-1 Route to default dataNode
            String dataNode = this.schema.getDataNode();
            if (dataNode == null) {
                String s = this.serverConnection.getSchema();
                s = String.format("No route in table '%s'.'%s'", s, this.tableName);
                SQLNonTransientException cause = new SQLNonTransientException(s);
                throw new FrontendException("Route failed", cause);
            }
            RouteResultsetNode rrNode = new RouteResultsetNode(dataNode, sqlType, this.sql);
            rrs.setNodes(new RouteResultsetNode[]{rrNode});
        } else if (this.tableConfig.isGlobalTable()) {
            // Case-2 Route to all dataNodes for global table
            List<String> dataNodes = this.tableConfig.getDataNodes();
            int n = dataNodes.size();
            RouteResultsetNode[] rrsNodes = new RouteResultsetNode[n];

            for (int i = 0; i < n; i++) {
                String dataNode = dataNodes.get(i);
                RouteResultsetNode rrNode = new RouteResultsetNode(dataNode, sqlType, this.sql);
                rrsNodes[i] = rrNode;
            }
            rrs.setNodes(rrsNodes);
        }

        if (!rrs.isEmpty()) {
            String file;

            if (this.statement.isLocal()) {
                file = this.tempFile;
            } else {
                file = this.loadData.getFileName();
            }
            for (RouteResultsetNode rrn: rrs.getNodes()) {
                String node = rrn.getName();
                initNodeData(node, file);
            }
            this.routeResultset = rrs;
            this.routeComplete = true;
            log.debug("route complete for 'load data' infile {}: node count {}",
                    this.fileName, rrs.size());
        }

        return rrs;
    }

    private void parseOneLine(List<SQLExpr> columns, String tableName, String[] values,
                              String lineSep, int rowIndex) {

        RouteResultset rrs = routeByShardColumn(this.sql, values, rowIndex);
        if (rrs.isEmpty()) {
            String insert = makeSimpleInsert(columns, values, tableName, true);
            rrs = this.serverConnection.routeSQL(insert, ServerParse.INSERT);
        }
        if (rrs == null || rrs.isEmpty()) {
            // 路由已处理
            return;
        }

        for (RouteResultsetNode routeResultsetNode : rrs.getNodes()) {
            String node = routeResultsetNode.getName();
            LoadData data = initNodeData(node, null);
            String row = joinValues(values, data) + lineSep;
            appendToNodeFile(node, data, row);
        }
    }

    private LoadData initNodeData(String node, String fileName) {
        LoadData data = this.routeResultMap.get(node);

        if (data == null) {
            data = new LoadData();
            data.setCharset(this.loadData.getCharset());
            data.setEnclose(this.loadData.getEnclose());
            data.setFieldTerminatedBy(this.loadData.getFieldTerminatedBy());
            data.setLineTerminatedBy(this.loadData.getLineTerminatedBy());
            data.setEscape(this.loadData.getEscape());
            data.setFileName(fileName);
            this.routeResultMap.put(node, data);
        }

        return data;
    }

    private void flushNodeFiles() {
        IOException lastError = null;

        for (Map.Entry<String, LoadData> it : this.routeResultMap.entrySet()) {
            String node = it.getKey();
            IOException error = flushNodeFile(node);
            if (error != null) {
                lastError = error;
            }
        }

        if (lastError != null) {
            throw new RuntimeException("Flush node data failed", lastError);
        }
    }

    private IOException flushNodeFile(String node) {
        OutputStream outFile = this.nodeOutFiles.get(node);

        if (outFile != null) {
            try {
                outFile.flush();
            } catch (IOException e) {
                return e;
            } finally {
                IoUtil.close(outFile);
                this.nodeOutFiles.remove(node);
            }
        }

        return null;
    }

    private void appendToNodeFile(String node, LoadData data, String row) {
        if (data.getFileName() == null) {
            String dnPath = this.tempPath + node + ".txt";
            data.setFileName(dnPath);
        }

        OutputStream outFile = this.nodeOutFiles.get(node);
        boolean failed = true;
        try {
            if (outFile == null) {
                File dnFile = new File(data.getFileName());
                outFile = IoUtil.fileOutputStream(dnFile, true);
                this.nodeOutFiles.put(node, outFile);
            }

            Charset cs = Charset.forName(this.loadData.getCharset());
            byte[] rowData = row.getBytes(cs);
            outFile.write(rowData);

            failed = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (failed) {
                IoUtil.close(outFile);
                this.nodeOutFiles.remove(node);
            }
        }
    }

    private String joinValues(String[] values, LoadData loadData) {
        String fieldSep = loadData.getFieldTerminatedBy();
        String enclose = loadData.getEnclose();
        String rep = loadData.getEscape() + enclose;
        StringBuilder sb = new StringBuilder();

        for (int i = 0, n = values.length; i < n; i++) {
            String s = values[i] != null? values[i]: "";

            if(i > 0) {
                sb.append(fieldSep);
            }
            if(enclose == null) {
                sb.append(s);
            } else {
                String escaped = s.replace(enclose, rep);
                sb.append(enclose).append(escaped).append(enclose);
            }
        }

        return sb.toString();
    }

    private RouteResultset buildResultSet(Map<String, LoadData> routeMap) {
        final int sqlType = ServerParse.LOAD_DATA_INFILE_SQL;

        this.statement.setLocal(true); // 强制local
        // 默认druid会过滤掉路径的分隔符，所以这里重新设置下
        SQLLiteralExpr fn = new SQLCharExpr(this.fileName);
        this.statement.setFileName(fn);

        String srcStatement = this.statement.toString();
        RouteResultset rrs = new RouteResultset(srcStatement, sqlType);
        rrs.setLoadData(true);
        rrs.setStatement(srcStatement);
        rrs.setAutocommit(this.serverConnection.isAutocommit());
        rrs.setFinishedRoute(true);

        int size = routeMap.size();
        int index = 0;
        RouteResultsetNode[] rrsNodes = new RouteResultsetNode[size];
        for (String dn : routeMap.keySet()) {
            RouteResultsetNode rrNode = new RouteResultsetNode(dn, sqlType, srcStatement);
            rrNode.setTotalNodeSize(size);
            rrNode.setStatement(srcStatement);

            LoadData newLoadData = new LoadData();
            ObjectUtil.copyProperties(this.loadData, newLoadData);
            newLoadData.setLocal(true);
            LoadData mapData = routeMap.get(dn);
            if (mapData.getFileName() != null) {
                // 此处使用分库load的临时文件dn1.txt/dn2.txt，或者clientTemp.txt（全局表、默认节点表）
                newLoadData.setFileName(mapData.getFileName());
            }
            rrNode.setLoadData(newLoadData);

            rrsNodes[index] = rrNode;
            index++;
        }
        rrs.setNodes(rrsNodes);

        return rrs;
    }

    private String makeSimpleInsert(List<SQLExpr> columns, String[] values,
                                    String table, boolean isAddEncose) {

        StringBuilder sb = new StringBuilder()
        .append(LoadData.loadDataHint)
        .append("insert into ").append(table.toUpperCase());

        // Columns optional
        if (columns != null && columns.size() > 0) {
            sb.append("(");
            int n = columns.size();
            for (int i = 0; i < n; i++) {
                SQLExpr column = columns.get(i);
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(column.toString());
            }
            sb.append(") ");
        }

        sb.append(" values(");
        String enclose = this.loadData.getEnclose();
        int n = values.length;
        for (int i = 0; i < n; i++) {
            String value = values[i];
            if (i > 0) {
                sb.append(",");
            }
            if (isAddEncose) {
                value = parseFieldString(value, enclose);
                sb.append("'").append(value).append("'");
            } else {
                sb.append(value);
            }
        }
        sb.append(")");

        return sb.toString();
    }

    private String parseFieldString(String value, String enclose) {
        if (enclose == null || "".equals(enclose) || value == null) {
            return value;
        } else if (value.startsWith(enclose) && value.endsWith(enclose)) {
            int len = enclose.length();
            return value.substring(len - 1, value.length() - len);
        }

        return value;
    }


    @Override
    public void end(byte packID) {
        // load in data EOF
        this.isStartLoadData = false;
        this.packID = packID;
        flushTempFile();

        if (!this.routeComplete) {
            String charset = this.loadData.getCharset();
            String lineSep = this.loadData.getLineTerminatedBy();
            log.debug("parse file '{}'", this.tempFile);
            parseFileByLine(this.tempFile, charset, lineSep);
            flushNodeFiles();
        }
        RouteResultset rrs = buildResultSet(this.routeResultMap);
        ServerSession session = this.serverConnection.getSession();
        session.execute(rrs, ServerParse.LOAD_DATA_INFILE_SQL);
    }

    private void parseFileByLine(String file, String encode, String split) {
        String lineSeq = this.loadData.getLineTerminatedBy();
        List<SQLExpr> columns = this.statement.getColumns();
        CsvParserSettings settings = new CsvParserSettings();

        settings.getFormat().setLineSeparator(lineSeq);
        settings.getFormat().setDelimiter(this.loadData.getFieldTerminatedBy().charAt(0));
        if(this.loadData.getEnclose() != null) {
            settings.getFormat().setQuote(this.loadData.getEnclose().charAt(0));
        }
        if(this.loadData.getEscape() != null) {
            settings.getFormat().setQuoteEscape(this.loadData.getEscape().charAt(0));
        }
        settings.getFormat().setNormalizedNewline(lineSeq.charAt(0));

        CsvParser parser = new CsvParser(settings);
        InputStream in = null;
        try {
            String[] row;
            int rowIndex = 0;

            in = IoUtil.fileInputStream(new File(file));
            Reader reader = new InputStreamReader(in, encode);
            parser.beginParsing(reader);
            while ((row = parser.parseNext()) != null) {
                parseOneLine(columns, this.tableName, row, lineSeq, ++rowIndex);
            }
            parser.stopParsing();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IoUtil.close(in);
        }
    }

    public void clear() {
        this.isStartLoadData = false;
        this.tableId2DataNodeCache = null;
        this.schema = null;
        this.tableConfig = null;
        this.packID = 0;
        this.tableName = null;
        this.partitionColumnIndex = -1;
        this.loadData = null;
        this.sql = null;
        this.fileName = null;
        this.statement = null;
        this.routeResultset = null;
        this.routeComplete = false;

        // Cleanup file resources
        IoUtil.close(this.tempOutFile);
        this.tempOutFile = null;
        if (this.tempFile != null) {
            deleteFile(this.tempFile);
        }
        flushNodeFiles();
        if (this.tempPath != null) {
            deleteFile(this.tempPath);
        }

        this.routeResultMap.clear();
    }

    @Override
    public byte getLastPackId() {
        return this.packID;
    }

    @Override
    public boolean isStartLoadData() {
        return this.isStartLoadData;
    }

    private String getPartitionColumn() {
        TableConfig tc = this.tableConfig;
        String pColumn;

        if (tc.isSecondLevel()
                && tc.getParentTC().getPartitionColumn().equals(tc.getParentKey())) {
            pColumn = tc.getJoinKey();
        } else {
            pColumn = tc.getPartitionColumn();
        }

        return pColumn;
    }

    /**
     * 删除目录及其所有子目录和文件
     *
     * @param dirPath 要删除的目录路径
     * @throws Exception
     */
    private static void deleteFile(String dirPath) {
        File fileDirToDel = new File(dirPath);
        if (!fileDirToDel.exists()) {
            return;
        }
        if (fileDirToDel.isFile()) {
            fileDirToDel.delete();
            return;
        }

        File[] fileList = fileDirToDel.listFiles();
        for (int i = 0; i < fileList.length; i++) {
            File file = fileList[i];
            if (file.isFile() && file.exists()) {
                file.delete();
            } else if (file.isDirectory()) {
                deleteFile(file.getAbsolutePath());
                file.delete();
            }
        }

        fileDirToDel.delete();
    }

}