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
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.univocity.parsers.csv.CsvFormat;
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
import org.opencloudb.net.NioProcessor;
import org.opencloudb.net.ResourceTask;
import org.opencloudb.net.handler.LoadDataInfileHandler;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.net.mysql.RequestFilePacket;
import org.opencloudb.parser.druid.DruidShardingParseInfo;
import org.opencloudb.parser.druid.MycatStatementParser;
import org.opencloudb.parser.druid.RouteCalculateUnit;
import org.opencloudb.route.MyCATSequenceProcessor;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.ServerSession;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.*;
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
    private LoadData loadData;

    private Map<String, String> routeResultMap = new HashMap<>();
    private RouteResultset routeResultset;
    private boolean routeComplete;

    private Map<String, OutputStream> nodeOutFiles = new HashMap<>();
    private String tempPath;
    private String tempFile;
    private OutputStream tempOutFile;

    private String tableName;
    private TableConfig tableConfig;
    private int autoIncrColumnIndex = -1;
    private int partitionColumnIndex = -1;
    private IdFetcher idFetcher;

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
            sendError(ErrorCode.ER_FILE_NOT_FOUND, s);
            return;
        }

        MycatServer server = MycatServer.getContextServer();
        String schemaName = this.serverConnection.getSchema();
        this.schema = server.getConfig().getSchemas().get(schemaName);
        this.tableId2DataNodeCache = (LayeredCachePool) server.getCacheService()
                .getCachePool("TableID2DataNodeCache");
        this.tableName = this.statement.getTableName().getSimpleName().toUpperCase();
        this.tableConfig = this.schema.getTables().get(this.tableName);
        List<SQLExpr> columns = this.statement.getColumns();
        if (this.tableConfig != null && this.tableConfig.isAutoIncrement() && columns != null) {
            String pk = this.tableConfig.getPrimaryKey();
            int n = columns.size();
            for (int i = 0; i < n; ++i) {
                String c = columns.get(i).toString();
                if (c.equalsIgnoreCase(pk)) {
                    this.autoIncrColumnIndex = i;
                    break;
                }
            }
            if (this.autoIncrColumnIndex == -1) {
                this.autoIncrColumnIndex = n; // add ID
            }
        }

        // tmp: $MYCAT_HOME/temp/SOURCE-ID/
        this.tempPath = SystemConfig.getHomePath() + File.separator + "temp"
                + File.separator + this.serverConnection.getId() + File.separator;
        this.tempFile = this.tempPath + "clientTemp.txt";
        // Note: It's possible that old files existing in temp path. So it's important that
        // first clearing temp files after init temp directory, otherwise appending into old files
        // can lead to data duplicated.
        clearTempFiles();

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
                if (this.partitionColumnIndex == -1 && this.autoIncrColumnIndex != -1) {
                    String pk = this.tableConfig.getPrimaryKey();
                    if (pk.equalsIgnoreCase(getPartitionColumn())) {
                        this.partitionColumnIndex = this.autoIncrColumnIndex;
                    }
                }
            }
        }

        initLoadDataParams();
        routeByTable();

        String charset = this.statement.getCharset();
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
            sendError(ErrorCode.ER_FILE_NOT_FOUND, s);
            return;
        }
        if (!infile.canRead()) {
            String s = "'" + this.fileName + "' can't read!";
            sendError(ErrorCode.ER_ERROR_ON_READ, s);
            return;
        }

        doLoad(this.fileName);
    }

    @Override
    public void handle(byte[] data) {
        BinaryPacket packet;

        boolean failed = true;
        try {
            if (this.sql == null) {
                String s = "Unknown command";
                sendError(ErrorCode.ER_UNKNOWN_COM_ERROR, s);
                return;
            }

            ByteArrayInputStream in = new ByteArrayInputStream(data);
            packet = new BinaryPacket();
            packet.read(in);
            failed = false;
        } catch (IOException e) {
            throw new FrontendException("'load data infile' data error", e);
        } finally {
            if (failed) {
                clear();
            }
        }

        appendToTempFile(packet.data);
    }

    @Override
    public void end(byte packID) {
        // load in data EOF
        this.isStartLoadData = false;
        this.packID = packID;

        flushTempFile();
        doLoad(this.tempFile);
    }

    void doLoad(String fileName) {
        if (this.routeComplete) {
            execute(fileName);
            return;
        }

        String charset = this.loadData.getCharset();
        String lineSep = this.loadData.getLineTerminatedBy();
        log.debug("parse file '{}'", fileName);
        parseFileByLine(fileName, charset, lineSep);
        if (this.idFetcher == null) {
            execute(fileName);
        }
    }

    void execute(String fileName) {
        flushNodeFiles();
        RouteResultset rrs = buildResultSet(fileName, this.routeResultMap);
        ServerSession session = this.serverConnection.getSession();
        this.isStartLoadData = false;
        log.debug("loading data into backend nodes");
        session.execute(rrs, ServerParse.LOAD_DATA_INFILE_SQL);
    }

    void sendError(Throwable cause) {
        String s = ExceptionUtil.getClientMessage(cause);
        sendError(s);
    }

    void sendError(String error) {
        try {
            this.serverConnection.writeErrMessage(ErrorCode.ER_YES, error);
        } finally {
            clear();
        }
    }

    void sendError(int errno, String errmsg) {
        try {
            this.serverConnection.writeErrMessage(errno, errmsg);
        } finally {
            clear();
        }
    }

    private void initLoadDataParams() {
        this.loadData = new LoadData();
        this.loadData.setFileName(this.fileName);

        SQLTextLiteralExpr rawLineEnd = (SQLTextLiteralExpr) this.statement.getLinesTerminatedBy();
        this.loadData.setLineTerminatedBy(rawLineEnd == null? "\n": rawLineEnd.getText());

        SQLTextLiteralExpr rawFieldEnd = (SQLTextLiteralExpr) this.statement.getColumnsTerminatedBy();
        this.loadData.setFieldTerminatedBy(rawFieldEnd == null? "\t": rawFieldEnd.getText());

        SQLTextLiteralExpr escapeExpr =  (SQLTextLiteralExpr) this.statement.getColumnsEscaped();
        this.loadData.setEscape(escapeExpr == null? "\\": escapeExpr.getText());

        SQLTextLiteralExpr rawEnclosed = (SQLTextLiteralExpr) this.statement.getColumnsEnclosedBy();
        this.loadData.setEnclose(rawEnclosed == null ? null : rawEnclosed.getText());

        String charset = this.statement.getCharset();
        if (charset == null) {
            charset = this.serverConnection.getCharset();
            this.statement.setCharset(charset);
        }
        this.loadData.setCharset(charset);
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

    private RouteResultset routeByShardColumn(String sql, String[] values, int rowIndex, Long id) {
        final int sqlType = ServerParse.INSERT;
        RouteResultset rrs = new RouteResultset(sql, sqlType);
        rrs.setLoadData(true);

        // Route to specified dataNodes for shard table by partition rule
        int pi = this.partitionColumnIndex, ai = this.autoIncrColumnIndex;
        if (pi == -1 || (pi >= values.length && pi != ai)) {
            String s = this.serverConnection.getSchema();
            s = String.format("No partition column in table '%s'.'%s'", s, this.tableName);
            SQLNonTransientException cause = new SQLNonTransientException(s);
            throw new FrontendException("Route failed", cause);
        }
        DruidShardingParseInfo ctx = new DruidShardingParseInfo();
        ctx.addTable(this.tableName);
        String value, column;
        if (id == null || pi != ai) {
            value = values[this.partitionColumnIndex];
            column = getPartitionColumn();
        } else {
            // Shard by ID
            value = id + "";
            column= this.tableConfig.getPrimaryKey();
        }
        RouteCalculateUnit calcUnit = new RouteCalculateUnit();
        String enclosed = this.loadData.getEnclose();
        value = parseFieldString(value, enclosed);
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
            rrs.setGlobalTable(true);
        }

        if (!rrs.isEmpty()) {
            String fileName = getLoadFileName();

            for (RouteResultsetNode rrn: rrs.getNodes()) {
                String node = rrn.getName();
                initNodeFile(node, fileName);
            }
            this.routeResultset = rrs;
            this.routeComplete = true;
            log.debug("route complete for 'load data' infile {}: node count {}",
                    this.fileName, rrs.size());
        }

        return rrs;
    }

    String getLoadFileName() {
        String fileName;

        if (this.statement.isLocal()) {
            fileName = this.tempFile;
        } else {
            fileName = this.fileName;
        }

        return fileName;
    }

    private boolean parseOneLine(CsvParser parser, List<SQLExpr> columns, String tableName, String[] values,
                                 String lineSep, int rowIndex) {

        int i = this.autoIncrColumnIndex;
        if (i != -1) {
            String id = null;
            if (i < values.length) {
                id = values[i];
            }
            if (id == null) {
                // First generate ID value before route
                MycatServer server = MycatServer.getContextServer();
                MyCATSequenceProcessor seqProcessor = server.getSequenceProcessor();
                if (this.idFetcher == null) {
                    log.debug("Auto increment ID not specified, so fetch it");
                    ServerLoadDataInfileHandler handler = this;
                    this.idFetcher = new IdFetcher(handler, parser, columns, tableName, values, lineSep, rowIndex);
                } else {
                    this.idFetcher.reset(values, rowIndex);
                }
                seqProcessor.nextValue(tableName, this.idFetcher);
                return true;
            }
        }

        routeByColumn(columns, tableName, values, lineSep, rowIndex, null);
        return false;
    }

    private void routeByColumn(List<SQLExpr> columns, String tableName, String[] values,
                              String lineSep, int rowIndex, Long id) {

        RouteResultset rrs = routeByShardColumn(this.sql, values, rowIndex, id);
        if (rrs.isEmpty()) {
            String insert = makeSimpleInsert(columns, values, tableName, true, id);
            rrs = this.serverConnection.routeSQL(insert, ServerParse.INSERT);
        }
        if (rrs == null || rrs.isEmpty()) {
            // 路由已处理
            return;
        }

        for (RouteResultsetNode rrn : rrs.getNodes()) {
            String node = rrn.getName();
            String row = joinValues(values, id) + lineSep;
            appendToNodeFile(node, row);
        }
    }

    private String initNodeFile(String node, String fileName) {
        String dataFile = this.routeResultMap.get(node);

        if (dataFile == null) {
            this.routeResultMap.put(node, fileName);
            return fileName;
        } else {
            return dataFile;
        }
    }

    private void flushNodeFiles() {
        IOException lastError = null;

        for (Map.Entry<String, String> it : this.routeResultMap.entrySet()) {
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

    private void appendToNodeFile(String node, String row) {
        String nodeFile = this.routeResultMap.get(node);
        if (nodeFile == null) {
            nodeFile = this.tempPath + node + ".txt";
            this.routeResultMap.put(node, nodeFile);
            log.debug("append data to node file '{}'", nodeFile);
        }

        OutputStream outFile = this.nodeOutFiles.get(node);
        boolean failed = true;
        try {
            if (outFile == null) {
                File dnFile = new File(nodeFile);
                outFile = IoUtil.fileOutputStream(dnFile, true);
                this.nodeOutFiles.put(node, outFile);
            }

            Charset cs = Charset.forName(this.statement.getCharset());
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

    private String joinValues(String[] values, Long id) {
        String fieldSep = this.loadData.getFieldTerminatedBy();
        String enclose = this.loadData.getEnclose();
        String rep = this.loadData.getEscape() + enclose;
        StringBuilder sb = new StringBuilder();

        int n = values.length;
        boolean prependId = false;
        if (this.autoIncrColumnIndex >= n) {
            sb.append(id);
            prependId = true;
        }
        for (int i = 0; i < n; i++) {
            String s = values[i] != null? values[i]: "";

            if(i > 0 || prependId) {
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

    private RouteResultset buildResultSet(String fileName, Map<String, String> routeMap) {
        final int sqlType = ServerParse.LOAD_DATA_INFILE_SQL;

        this.statement.setLocal(true); // 强制local
        // 默认druid会过滤掉路径的分隔符，所以这里重新设置下
        SQLLiteralExpr fn = new SQLCharExpr(fileName);
        this.statement.setFileName(fn);
        List<SQLExpr> columns = this.statement.getColumns();
        if (columns != null && this.autoIncrColumnIndex >= columns.size()) {
            List<SQLExpr> newColumns = new ArrayList<>(columns.size() + 1);
            // Prepend ID
            newColumns.add(new SQLIdentifierExpr(this.tableConfig.getPrimaryKey()));
            newColumns.addAll(columns);
            this.statement.setColumns(newColumns);
        }

        String srcStatement = this.statement.toString();
        RouteResultset rrs = new RouteResultset(srcStatement, sqlType);
        rrs.setLoadData(true);
        rrs.setStatement(srcStatement);
        rrs.setAutocommit(this.serverConnection.isAutocommit());
        rrs.setFinishedRoute(true);
        if (this.routeResultset != null) {
            rrs.setGlobalTable(this.routeResultset.isGlobalTable());
        }

        int size = routeMap.size();
        int index = 0;
        RouteResultsetNode[] rrsNodes = new RouteResultsetNode[size];
        for (String dn : routeMap.keySet()) {
            String nodeFile = routeMap.get(dn);
            SQLCharExpr fnExpr = new SQLCharExpr(nodeFile);
            this.statement.setFileName(fnExpr);
            String nodeStmt = this.statement.toString();

            RouteResultsetNode rrn = new RouteResultsetNode(dn, sqlType, nodeStmt);
            rrn.setTotalNodeSize(size);
            rrn.setLoadFile(nodeFile);

            rrsNodes[index] = rrn;
            index++;
        }
        rrs.setNodes(rrsNodes);

        return rrs;
    }

    private String makeSimpleInsert(List<SQLExpr> columns, String[] values,
                                    String table, boolean isAddEncose, Long id) {

        StringBuilder sb = new StringBuilder()
        .append(LoadData.loadDataHint)
        .append("insert into ").append(table.toUpperCase());

        // Columns optional
        if (columns != null && columns.size() > 0) {
            sb.append("(");
            int n = columns.size();
            boolean prependId = false;
            if (this.autoIncrColumnIndex >= n) {
                sb.append(this.tableConfig.getPrimaryKey());
                prependId = true;
            }
            for (int i = 0; i < n; i++) {
                SQLExpr column = columns.get(i);
                if (i > 0 || prependId) {
                    sb.append(",");
                }
                sb.append(column.toString());
            }
            sb.append(") ");
        }

        sb.append(" values(");
        String enclose = this.loadData.getEnclose();
        int n = values.length;
        boolean prependId = false;
        if (this.autoIncrColumnIndex >= n) {
            sb.append(id);
            prependId = true;
        }
        for (int i = 0; i < n; i++) {
            String value = values[i];
            if (i > 0 || prependId) {
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
        } else {
            return value;
        }
    }

    private void parseFileByLine(String file, String encode, String lineSep) {
        List<SQLExpr> columns = this.statement.getColumns();
        CsvParserSettings settings = new CsvParserSettings();
        CsvFormat format = settings.getFormat();

        format.setLineSeparator(lineSep);
        format.setDelimiter(this.loadData.getFieldTerminatedBy().charAt(0));
        if(this.loadData.getEnclose() != null) {
            format.setQuote(this.loadData.getEnclose().charAt(0));
        }
        if(this.loadData.getEscape() != null) {
            format.setQuoteEscape(this.loadData.getEscape().charAt(0));
        }
        format.setNormalizedNewline(lineSep.charAt(0));

        CsvParser parser = new CsvParser(settings);
        InputStream in = null;
        boolean failed = true;
        try {
            String table = this.tableName;
            String[] row;
            int rowIndex = 0;

            in = IoUtil.fileInputStream(new File(file));
            Reader reader = new InputStreamReader(in, encode);
            parser.beginParsing(reader);
            while ((row = parser.parseNext()) != null) {
                boolean genId = parseOneLine(parser, columns, table, row, lineSep, ++rowIndex);
                if (genId) {
                    failed = false;
                    return;
                }
            }
            parser.stopParsing();
            failed = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (failed || this.idFetcher == null) {
                IoUtil.close(in);
                this.idFetcher = null;
            }
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

        IoUtil.close(this.idFetcher);
        this.idFetcher = null;

        clearTempFiles();
        this.routeResultMap.clear();
    }

    protected void clearTempFiles() {
        IoUtil.close(this.tempOutFile);
        this.tempOutFile = null;
        if (this.tempFile != null) {
            deleteFile(this.tempFile);
        }

        flushNodeFiles();
        if (this.tempPath != null) {
            deleteFile(this.tempPath);
        }
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

    static class IdFetcher implements Callback<Long>, ResourceTask, AutoCloseable {
        final ServerLoadDataInfileHandler handler;
        final CsvParser parser;

        final List<SQLExpr> columns;
        final String table;
        final String lineSeq;

        String[] row;
        int rowIndex;
        int recurDepth;

        public IdFetcher(ServerLoadDataInfileHandler handler, CsvParser parser,
                         List<SQLExpr> columns, String table, String[] row, String lineSeq, int rowIndex) {
            this.handler = handler;
            this.parser = parser;
            this.columns = columns;
            this.table = table;
            this.row = row;
            this.lineSeq = lineSeq;
            this.rowIndex = rowIndex;
        }

        public void reset(String[] row, int rowIndex) {
            this.row = row;
            this.rowIndex = rowIndex;
        }

        @Override
        public void call(Long result, Throwable cause) {
            boolean failed = true;
            try {
                this.recurDepth++;
                if (cause != null) {
                    handler.sendError(cause);
                    return;
                }

                log.debug("ID {} fetched, then do route", result);
                if (handler.autoIncrColumnIndex < this.row.length) {
                    this.row[handler.autoIncrColumnIndex] = result + "";
                }
                handler.routeByColumn(this.columns, this.table, this.row, this.lineSeq, this.rowIndex, result);

                if (this.recurDepth > 50) {
                    log.debug("recursion depth exceeds {}: execute later", this.recurDepth);
                    this.recurDepth = 0;
                    NioProcessor processor = NioProcessor.ensureRunInProcessor();
                    processor.executeLater(this);
                } else {
                    run();
                }

                failed = false;
            } catch (Throwable e) {
                handler.sendError(cause);
            } finally {
                if (failed) {
                    this.parser.stopParsing();
                }
            }
        }

        @Override
        public void release(String reason) {
            close();
            log.debug("stop csv parser: {}", reason);
        }

        @Override
        public void run() {
            // Next line
            boolean genId;
            do {
                this.row = this.parser.parseNext();
                if (this.row == null) {
                    String fileName = this.handler.getLoadFileName();
                    this.handler.execute(fileName);
                    return;
                }
                this.rowIndex++;
                genId = this.handler.parseOneLine(this.parser, this.columns,
                        this.table, this.row, this.lineSeq, this.rowIndex);
            } while (!genId);
        }

        @Override
        public void close() {
            this.parser.stopParsing();
        }

    }

}