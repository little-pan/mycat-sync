package demo.catlets;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.QName;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.util.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * aStoneGod 2015.11
 */
public class ZkDownload {

    private static final String ZK_CONFIG_FILE_NAME = "zk-create.yaml";
    private static final Logger log = LoggerFactory.getLogger(ZkDownload.class);

    private static final String SERVER_CONFIG_DIRECTORY = "server-config";
    private static final String DATANODE_CONFIG_DIRECTORY = "datanode-config";
    private static final String RULE_CONFIG_DIRECTORY = "rule-config";
    private static final String SEQUENCE_CONFIG_DIRECTORY = "sequence-config";
    private static final String SCHEMA_CONFIG_DIRECTORY = "schema-config";
    private static final String DATAHOST_CONFIG_DIRECTORY = "datahost-config";
    private static final String MYSQLREP_CONFIG_DIRECTORY = "mysqlrep-config";


    private static final String CONFIG_ZONE_KEY = "zkZone";
    private static final String CONFIG_URL_KEY = "zkUrl";
    private static final String CONFIG_CLUSTER_KEY = "zkClu";
    private static final String CONFIG_MYCAT_ID = "zkID";
    private static final String CONFIG_SYSTEM_KEY = "system";
    private static final String CONFIG_USER_KEY = "user";
    private static final String CONFIG_DATANODE_KEY = "datanode";
    private static final String CONFIG_RULE_KEY = "rule";
    private static final String CONFIG_SEQUENCE_KEY = "sequence";
    private static final String CONFIG_SCHEMA_KEY = "schema";
    private static final String CONFIG_DATAHOST_KEY = "datahost";
    private static final String CONFIG_MYSQLREP_KEY = "mysqlrep";

    private static String CLU_PARENT_PATH;
    private static String ZONE_PARENT_PATH;
    private static String SERVER_PARENT_PATH;


    private static CuratorFramework framework;
    private static Map<String, Object> zkConfig;

    private static File downloadDir;

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java demo.catlets.ZkDownload <download dir>");
            System.exit(1);
        }
        File dir = new File(args[0]);
        if (!dir.isDirectory()) {
            System.err.println("'"+dir+"' not existing");
            System.exit(1);
        }
        downloadDir = dir;

        init();
    }

    public static boolean init()  {
        log.info("start zkdownload to local xml");
        zkConfig = loadZkConfig();
        
        ZONE_PARENT_PATH = ZKPaths.makePath("/", String.valueOf(zkConfig.get(CONFIG_ZONE_KEY)));
        CLU_PARENT_PATH = ZKPaths.makePath(ZONE_PARENT_PATH + "/", String.valueOf(zkConfig.get(CONFIG_CLUSTER_KEY)));
        SERVER_PARENT_PATH = ZKPaths.makePath(ZONE_PARENT_PATH + "/", String.valueOf(zkConfig.get(CONFIG_CLUSTER_KEY)+"/"+String.valueOf(zkConfig.get(CONFIG_MYCAT_ID))));
        log.trace("parent path is {}", CLU_PARENT_PATH);
        framework = createConnection((String) zkConfig.get(CONFIG_URL_KEY));
        try {
        	boolean exitsZk = isHavingConfig();
        	if(exitsZk){
        		List<Map<String, JSONObject>> listDataNode = getDatanodeConfig(DATANODE_CONFIG_DIRECTORY);
                List<Map<String, JSONObject>> listDataHost = getDataHostNodeConfig(CLU_PARENT_PATH,DATAHOST_CONFIG_DIRECTORY);
                List<Map<String, JSONObject>> listServer = getServerNodeConfig(SERVER_CONFIG_DIRECTORY);
                List<Map<String, JSONObject>> listSchema = getSchemaConfig(SCHEMA_CONFIG_DIRECTORY);
                List<Map<String, JSONObject>> listRule = getServerNodeConfig(RULE_CONFIG_DIRECTORY);

                //生成SERVER XML
                processServerDocument(listServer);

                //生成SCHEMA XML
                processSchemaDocument(listSchema);

                //生成RULE XML
                processRuleDocument(listRule);
        	}else{
        		return false;
        	}
        }catch (Exception e) {
            log.warn("start zkdownload to local error,",e);
        }
        
        return true;
    }

    public static Set<Map<String,JSONObject>> getMysqlRep(List<Map<String, JSONObject>> listMysqlRep,
                                                          String trepid) {
        Set<Map<String, JSONObject>> set = new HashSet<>();
        String[] repids = trepid.split(",");
        for (String repid : repids){
            for (int i=0; i<listMysqlRep.size();i++){
                String datahostName = listMysqlRep.get(i).keySet().toString()
                        .replace("[", "").replace("]", "").trim();
                if (datahostName.contains(repid))
                    set.add(listMysqlRep.get(i));
            }
        }
        return set;
    }

    public static void processMysqlRepDocument(Element serverElement,List<Map<String,JSONObject>> mapList)
            throws Exception {

        for (int i = 0;i < mapList.size(); i++) {
            int subLength = CLU_PARENT_PATH.length()+DATAHOST_CONFIG_DIRECTORY.length()+2;
            int repLength = ZONE_PARENT_PATH.length()+MYSQLREP_CONFIG_DIRECTORY.length()+2;
            String datahostName = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
            if (!datahostName.substring(subLength).contains("/")) {
                String key =datahostName.substring(subLength);
                JSONObject jsonObject = mapList.get(i).get(datahostName);
                Element dataHost = serverElement.addElement("dataHost");
                if (!key.isEmpty()){
                    Element datahost = dataHost.addAttribute("name", key);
                    if (jsonObject.has("writetype"))
                        datahost.addAttribute("writeType",jsonObject.get("writetype").toString());
                    if (jsonObject.has("switchType"))
                        datahost.addAttribute("switchType",jsonObject.get("switchType").toString());
                    if (jsonObject.has("slaveThreshold"))
                        datahost.addAttribute("slaveThreshold",jsonObject.get("slaveThreshold").toString());
                    if (jsonObject.has("balance"))
                        datahost.addAttribute("balance",jsonObject.get("balance").toString());
                    if (jsonObject.has("dbtype"))
                        datahost.addAttribute("dbType",jsonObject.get("dbtype").toString());
                    if (jsonObject.has("maxcon"))
                        datahost.addAttribute("maxCon",jsonObject.get("maxcon").toString());
                    if (jsonObject.has("mincon"))
                        datahost.addAttribute("minCon",jsonObject.get("mincon").toString());
                    if (jsonObject.has("dbDriver"))
                        datahost.addAttribute("dbDriver",jsonObject.get("dbDriver").toString());
                    if (jsonObject.has("heartbeatSQL")){
                        Element  heartbeatSQL = dataHost.addElement("heartbeat");
                        heartbeatSQL.setText(jsonObject.get("heartbeatSQL").toString());
                    }

                    String repid = jsonObject.get("repid").toString();
                    List<Map<String, JSONObject>> listMysqlRep = getDataHostNodeConfig(ZONE_PARENT_PATH,MYSQLREP_CONFIG_DIRECTORY);
                    Set<Map<String,JSONObject>> datahostSet = getMysqlRep(listMysqlRep,repid);
                    Iterator<Map<String,JSONObject>> it = datahostSet.iterator();
                    //处理WriteHost
                    for (Map<String,JSONObject> wdh : datahostSet) {
                        String host = wdh.keySet().toString().replace("[", "").replace("]", "").trim();
                        String temp = host.substring(repLength, host.length());
                        if (temp.contains("/")&&!temp.contains("readHost")) {
                            String currepid = temp.substring(0,temp.indexOf("/"));
                            String childHost = temp.substring(temp.indexOf("/")+1, temp.length());
                            JSONObject childJsonObject = wdh.get(host);
                            Element writeHost = dataHost.addElement("writeHost");
                            if (childJsonObject.has("host"))
                                writeHost.addAttribute("host", childJsonObject.get("host").toString());
                            if (childJsonObject.has("url"))
                                writeHost.addAttribute("url", childJsonObject.get("url").toString());
                            if (childJsonObject.has("user"))
                                writeHost.addAttribute("user", childJsonObject.get("user").toString());
                            if (childJsonObject.has("password"))
                                writeHost.addAttribute("password", childJsonObject.get("password").toString());
                            //处理readHost
                            for (Map<String,JSONObject> rdh : datahostSet) {
                                String readhost = rdh.keySet().toString().replace("[", "").replace("]", "").trim();
                                if (readhost.equals(host)||readhost.compareTo(host)<=0||!readhost.contains(currepid))
                                    continue;
                                String tempread = readhost.substring(host.length()-childHost.length(), readhost.length());
                                if (tempread.contains("/") && tempread.contains(childHost)) {
                                    JSONObject readJsonObject = rdh.get(readhost);
                                    Element readHostEl = writeHost.addElement("readHost");
                                    if (readJsonObject.has("host"))
                                        readHostEl.addAttribute("host", readJsonObject.get("host").toString());
                                    if (readJsonObject.has("url"))
                                        readHostEl.addAttribute("url", readJsonObject.get("url").toString());
                                    if (readJsonObject.has("user"))
                                        readHostEl.addAttribute("user", readJsonObject.get("user").toString());
                                    if (readJsonObject.has("password"))
                                        readHostEl.addAttribute("password", readJsonObject.get("password").toString());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    // config txt file
    public static void conf2File(String fileName, String config) {
        OutputStream out = null;
        BufferedWriter fw = null;
        File file = new File(downloadDir, fileName);
        if (file.isFile()) {
            throw new ConfigException("'"+fileName+"' is existing in directory '"+downloadDir+"'");
        }

        try {
            out = new FileOutputStream(file, true);
            // 指定编码格式，以免读取时中文字符异常
            fw = new BufferedWriter(new OutputStreamWriter(out, SystemConfig.CHARSET));
            String[] configs = config.split(",");
            for (String con:configs){
                fw.write(con);
                fw.newLine();
            }
            fw.flush(); // 全部写入缓存中的内容
        } catch (Exception e) {
            throw new ConfigException("Fatal: write config file '"+file+"'", e);
        } finally {
            IoUtil.close(fw);
            IoUtil.close(out);
        }
    }

    //zk Config
    public static boolean isHavingConfig()throws Exception {
        String nodePath = CLU_PARENT_PATH ;
        log.trace("child path is {}", nodePath);
        List list = null;
        try {
            list  = framework.getChildren().forPath(nodePath);
		} catch(NoNodeException e){
            log.warn("remote zk center not exists node :" + nodePath  );
        	return false;
        }
        if(list!=null && list.size()>0){
        	return true;
        }

        return false;
    }
    
    //Datanode Config
    public static List<Map<String,JSONObject>> getDatanodeConfig(String configKey)throws Exception {
        String nodePath = CLU_PARENT_PATH + "/" + configKey;
        log.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getDatanodeConfig(listServer, nodePath);
        return listServer;
    }

    //Datanode Child Config
    private static List<Map<String,JSONObject>> getDatanodeConfig(List<Map<String,JSONObject>> listServer,
                                                                  String childPath) throws Exception {
        List<String> list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        if (nodeSize==0 ){
            //处理叶子节点数据
            listServer=getConfigData(listServer, childPath);
        }else  {
            for (int i = 0; i < nodeSize; i++) {
                String leaf = iterator.next();
                if (leaf.endsWith("config")){
                    listServer=getConfigData(listServer, childPath);
                }else {
                    String childPath1 = childPath + "/" + leaf;
                    getServerChildConfig(listServer, childPath1);
                }
            }
        }
        return listServer;
    }

    //Schema Config
    public static List<Map<String,JSONObject>> getSchemaConfig(String configKey)throws Exception {
        String nodePath = CLU_PARENT_PATH + "/" + configKey;
        log.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getSchemaChildConfig(listServer, nodePath);
        return listServer;
    }

    //Schema Child Config
    private static List<Map<String,JSONObject>> getSchemaChildConfig(List<Map<String,JSONObject>> listServer,
                                                                     String childPath) throws Exception {
        List list = new ArrayList<>();
        list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        for (int i = 0; i < nodeSize; i++) {
            String leaf = iterator.next();
            String childPath1 = childPath + "/" + leaf;
            listServer=getConfigData(listServer, childPath1);
            getSchemaChildConfig(listServer, childPath1);
        }
        return listServer;
    }

    //sequence Config
    public static List<Map<String,JSONObject>> getSequenceNodeConfig(String configKey)throws Exception {
        String nodePath = CLU_PARENT_PATH + "/" + configKey;
        log.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getSequenceChildConfig(listServer, nodePath);
        return listServer;
    }

    //Sequence Child Config
    private static List<Map<String,JSONObject>> getSequenceChildConfig(List<Map<String,JSONObject>> listServer,String childPath) throws Exception {

        List list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        if (nodeSize==0 ){
            //处理叶子节点数据
            listServer=getConfigData(listServer, childPath);
        }else  {
            for (int i = 0; i < nodeSize; i++) {
                String leaf = iterator.next();
                if (leaf.endsWith("config")){
                    listServer=getConfigData(listServer, childPath);
                    String childPath1 = childPath + "/" + leaf;
                    listServer=getConfigData(listServer, childPath1);
                }else if(leaf.endsWith("mapping")){
                    String childPath1 = childPath + "/" + leaf;
                    listServer=getConfigData(listServer, childPath1);
                }else  {
                    String childPath1 = childPath + "/" + leaf;
                    getSequenceChildConfig(listServer, childPath1);
                }
            }
        }
        return listServer;
    }



    //Server Config
    public static List<Map<String,JSONObject>> getServerNodeConfig(String configKey)throws Exception {
        String nodePath = CLU_PARENT_PATH + "/" + configKey;
        log.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getServerChildConfig(listServer, nodePath);
        return listServer;
    }

    //Server Child Config
    private static List<Map<String,JSONObject>> getServerChildConfig(List<Map<String,JSONObject>> listServer,
                                                                     String childPath) throws Exception {

        List list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        if (nodeSize==0 ){
            //处理叶子节点数据
            listServer=getConfigData(listServer, childPath);
        }else  {
            for (int i = 0; i < nodeSize; i++) {
                String leaf = iterator.next();
                if (leaf.endsWith("config")){
                    listServer=getConfigData(listServer, childPath);
                }else {
                    String childPath1 = childPath + "/" + leaf;
                    getServerChildConfig(listServer, childPath1);
                }
            }
        }
        return listServer;
    }

    //DataHost Config
    public static List<Map<String,JSONObject>> getDataHostNodeConfig(String parent_path,
                                                                     String configKey)throws Exception {
        String nodePath = parent_path + "/" + configKey;
        log.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getDataHostChildConfig(listServer, nodePath);
        return listServer;
    }

    //DataHost Child Config
    private static List<Map<String,JSONObject>> getDataHostChildConfig(List<Map<String,JSONObject>> listServer,
                                                                       String childPath) throws Exception {
        List list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        for (int i = 0; i < nodeSize; i++) {
            String leaf = iterator.next();
            String childPath1 = childPath + "/" + leaf;
            listServer=getConfigData(listServer, childPath1);
            getDataHostChildConfig(listServer, childPath1);
        }
        return listServer;
    }

    private static List<Map<String,JSONObject>> getConfigData(List<Map<String, JSONObject>> list,
                                                              String childPath) throws IOException {
        String data;
        try {
            data = new String(framework.getData().forPath(childPath), SystemConfig.CHARSET);
            if (data.startsWith("[")&&data.endsWith("]")){ //JsonArray
                JSONArray jsonArray = new JSONArray(data);
                for (int i=0;i<jsonArray.length();i++){
                    Map<String,JSONObject> map = new HashMap<>();
                    map.put(childPath,(JSONObject)jsonArray.get(i));
                    list.add(map);
                }
                return list;
            }else {
                JSONObject jsonObject = new JSONObject(data);
                Map<String,JSONObject> map = new HashMap<>();
                map.put(childPath,jsonObject);
                list.add(map);
                return list;
            }
        } catch (Exception e) {
            log.warn("get config data error", e);
        }
        return null;
    }

    //Server.xml
    public static void processServerDocument(List<Map<String,JSONObject>> mapList){
        /** 建立document对象 */
        Document document = DocumentHelper.createDocument();
        document.addDocType("mycat:server","","server.dtd");
        /** 建立serverElement根节点 */
        Element serverElement = document.addElement(QName.get("mycat:server ", "http://org.opencloudb/"));
        for (int i=0;i<mapList.size();i++){
            if (mapList.get(i).keySet().toString().contains("system")){
                String key = mapList.get(i).keySet().toString().replace("[","").replace("]","").trim();
                JSONObject jsonObject = mapList.get(i).get(key);
                Element system = serverElement.addElement("system");
                if (jsonObject.has("defaultsqlparser")){
                    Element defaultsqlparser = system.addElement("property").addAttribute("name", "defaultsqlparser");
                    defaultsqlparser.setText(jsonObject.getString("defaultsqlparser"));
                }
                if (jsonObject.has("serverport")){
                    Element serverport = system.addElement("property").addAttribute("name", "serverport");
                    serverport.setText(jsonObject.getString("serverport"));
                }
                if (jsonObject.has("sequncehandlertype")){
                    Element sequncehandlertype = system.addElement("property").addAttribute("name", "sequncehandlertype");
                    sequncehandlertype.setText(jsonObject.getString("sequncehandlertype"));
                }
                if (jsonObject.has("managerPort")){
                    Element managerPort = system.addElement("property").addAttribute("name", "managerPort");
                    managerPort.setText(jsonObject.getString("managerPort"));
                }
                if (jsonObject.has("charset")){
                    Element charset = system.addElement("property").addAttribute("name", "charset");
                    charset.setText(jsonObject.getString("charset"));
                }
                if (jsonObject.has("registryAddress")){
                    Element registryAddress = system.addElement("property").addAttribute("name", "registryAddress");
                    registryAddress.setText(jsonObject.getString("registryAddress"));
                }
                if (jsonObject.has("useCompression")){
                    Element useCompression = system.addElement("property").addAttribute("name", "useCompression");
                    useCompression.setText(jsonObject.getString("useCompression"));
                }
                if (jsonObject.has("processorBufferChunk")){
                    Element processorBufferChunk = system.addElement("property").addAttribute("name", "processorBufferChunk");
                    processorBufferChunk.setText(jsonObject.getString("processorBufferChunk"));
                }
                if (jsonObject.has("processors")){
                    Element processors = system.addElement("property").addAttribute("name", "processors");
                    processors.setText(jsonObject.getString("processors"));
                }
                if (jsonObject.has("processorExecutor")){
                    Element processorExecutor = system.addElement("property").addAttribute("name", "processorExecutor");
                    processorExecutor.setText(jsonObject.getString("processorExecutor"));
                }
                if (jsonObject.has("maxStringLiteralLength")){
                    Element maxStringLiteralLength = system.addElement("property").addAttribute("name", "maxStringLiteralLength");
                    maxStringLiteralLength.setText(jsonObject.getString("maxStringLiteralLength"));
                }
                if (jsonObject.has("sequnceHandlerType")){
                    Element sequnceHandlerType = system.addElement("property").addAttribute("name", "sequnceHandlerType");
                    sequnceHandlerType.setText(jsonObject.getString("sequnceHandlerType"));
                }
                if (jsonObject.has("backSocketNoDelay")){
                    Element backSocketNoDelay = system.addElement("property").addAttribute("name", "backSocketNoDelay");
                    backSocketNoDelay.setText(jsonObject.getString("backSocketNoDelay"));
                }
                if (jsonObject.has("frontSocketNoDelay")){
                    Element frontSocketNoDelay = system.addElement("property").addAttribute("name", "frontSocketNoDelay");
                    frontSocketNoDelay.setText(jsonObject.getString("frontSocketNoDelay"));
                }
                if (jsonObject.has("processorExecutor")){
                    Element processorExecutor = system.addElement("property").addAttribute("name", "processorExecutor");
                    processorExecutor.setText(jsonObject.getString("processorExecutor"));
                }
                if (jsonObject.has("mutiNodeLimitType")){
                    Element mutiNodeLimitType = system.addElement("property").addAttribute("name", "mutiNodeLimitType");
                    mutiNodeLimitType.setText(jsonObject.getString("mutiNodeLimitType"));
                }
                if (jsonObject.has("mutiNodePatchSize")){
                    Element mutiNodePatchSize = system.addElement("property").addAttribute("name", "mutiNodePatchSize");
                    mutiNodePatchSize.setText(jsonObject.getString("mutiNodePatchSize"));
                }
                if (jsonObject.has("idleTimeout")){
                    Element idleTimeout = system.addElement("property").addAttribute("name", "idleTimeout");
                    idleTimeout.setText(jsonObject.getString("idleTimeout"));
                }
                if (jsonObject.has("bindIp")){
                    Element bindIp = system.addElement("property").addAttribute("name", "bindIp");
                    bindIp.setText(jsonObject.getString("bindIp"));
                }
                if (jsonObject.has("frontWriteQueueSize")){
                    Element frontWriteQueueSize = system.addElement("property").addAttribute("name", "frontWriteQueueSize");
                    frontWriteQueueSize.setText(jsonObject.getString("frontWriteQueueSize"));
                }
            }else if (mapList.get(i).keySet().toString().contains("user")){
                String key = mapList.get(i).keySet().toString().replace("[","").replace("]","").trim();
                JSONObject jsonObject = mapList.get(i).get(key);
                Element user = serverElement.addElement("user").addAttribute("name", jsonObject.get("name").toString());
                if (jsonObject.has("password")) {
                    Element propertyUserEl = user.addElement("property").addAttribute("name", "password");
                    propertyUserEl.setText(jsonObject.get("password").toString());
                }
                if (jsonObject.has("schemas")) {
                    Element propertyUserEl1 = user.addElement("property").addAttribute("name", "schemas");
                    propertyUserEl1.setText(jsonObject.get("schemas").toString().replace("[\"","").replace("\"]",""));
                }
                if (jsonObject.has("readOnly")) {
                    Element propertyUserEl1 = user.addElement("property").addAttribute("name", "readOnly");
                    propertyUserEl1.setText(jsonObject.get("readOnly").toString());
                }
            }
        }
        json2XmlFile(document,"server.xml");
    }

    //rule.xml
    public static void processRuleDocument(List<Map<String,JSONObject>> mapList){
        /** 建立document对象 */
        Document document = DocumentHelper.createDocument();
        /** 建立serverElement根节点 */
        document.addDocType("mycat:rule","","rule.dtd");
        Element serverElement = document.addElement(QName.get("mycat:rule ", "http://org.opencloudb/"));
        for (int i=0;i<mapList.size();i++){
            String key = mapList.get(i).keySet().toString().replace("[","").replace("]","").trim();
            JSONObject jsonObject = mapList.get(i).get(key);
            // tableRule
            Element ruleEl = serverElement.addElement("tableRule").addAttribute("name",jsonObject.getString("name"));
            if (jsonObject.has("column")){
                Element ruleEl1 = ruleEl.addElement("rule");
                Element columns = ruleEl1.addElement("columns");
                columns.setText(jsonObject.getString("column"));

                if (jsonObject.has("name")) {
                    Element algorithm = ruleEl1.addElement("algorithm");
                    algorithm.setText(jsonObject.getString("name"));
                }
            }
        }

        //function
        for (int i=0;i<mapList.size();i++) {
            String key = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
            JSONObject jsonObject = mapList.get(i).get(key);
            Element system = serverElement.addElement("function");
            if (jsonObject.has("name")) {
                system.addAttribute("name", jsonObject.getString("name"));
            }
            if (jsonObject.has("functionName")) {
                //1.4 class
                String pathFor14 = "org.opencloudb.route.function";
                String func = jsonObject.getString("functionName");
                String className = pathFor14 + func.substring(func.lastIndexOf("."), func.length());
                system.addAttribute("class", className);
            }
            if (jsonObject.has("count")) {
                Element serverport = system.addElement("property").addAttribute("name", "count");
                serverport.setText(jsonObject.getString("count"));
            }
            if (jsonObject.has("virtualBucketTimes")) {
                Element serverport = system.addElement("property").addAttribute("name", "virtualBucketTimes");
                serverport.setText(jsonObject.getString("virtualBucketTimes"));
            }
            if (jsonObject.has("partitionCount")) {
                Element serverport = system.addElement("property").addAttribute("name", "partitionCount");
                serverport.setText(jsonObject.getString("partitionCount"));
            }
            if (jsonObject.has("partitionLength")) {
                Element serverport = system.addElement("property").addAttribute("name", "partitionLength");
                serverport.setText(jsonObject.getString("partitionLength"));
            }
            if (jsonObject.has("splitOneDay")) {
                Element serverport = system.addElement("property").addAttribute("name", "splitOneDay");
                serverport.setText(jsonObject.getString("splitOneDay"));
            }
            if (jsonObject.has("dateFormat")) {
                Element serverport = system.addElement("property").addAttribute("name", "dateFormat");
                serverport.setText(jsonObject.getString("dateFormat"));
            }
            if (jsonObject.has("sBeginDate")) {
                Element serverport = system.addElement("property").addAttribute("name", "sBeginDate");
                serverport.setText(jsonObject.getString("sBeginDate"));
            }
            if (jsonObject.has("type")) {
                Element serverport = system.addElement("property").addAttribute("name", "type");
                serverport.setText(jsonObject.getString("type"));
            }
            if (jsonObject.has("totalBuckets")) {
                Element serverport = system.addElement("property").addAttribute("name", "totalBuckets");
                serverport.setText(jsonObject.getString("totalBuckets"));
            }

            //mapFile from config
            if (jsonObject.has("config")) {
                String config = jsonObject.getString("config").replace("{", "").replace("}", "").replace("\"", "").replace(":", "=");
                String mapFile = jsonObject.getString("name");
                Element mapFileEl = system.addElement("property").addAttribute("name", "mapFile");
                mapFileEl.setText(mapFile + ".txt");
                conf2File("/" + mapFile + ".txt", config);
            }
            if (jsonObject.has("groupPartionSize")) {
                Element serverport = system.addElement("property").addAttribute("name", "groupPartionSize");
                serverport.setText(jsonObject.getString("groupPartionSize"));
            }
            if (jsonObject.has("sPartionDay")) {
                Element serverport = system.addElement("property").addAttribute("name", "sPartionDay");
                serverport.setText(jsonObject.getString("sPartionDay"));
            }
        }
        json2XmlFile(document,"rule.xml");
    }


    //Schema.xml
    public static void processSchemaDocument(List<Map<String,JSONObject>> mapList) throws Exception {

        Document document = DocumentHelper.createDocument();
        document.addDocType("mycat:schema","","schema.dtd");
        /** 建立serverElement根节点 */
        Element serverElement = document.addElement(QName.get("mycat:schema ", "http://org.opencloudb/"));
        for (int i=0;i<mapList.size();i++){
            int subLength = CLU_PARENT_PATH.length()+SCHEMA_CONFIG_DIRECTORY.length()+2;
            String SchemaPath = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
            if (!SchemaPath.substring(subLength,SchemaPath.length()).contains("/")){
                String schema =SchemaPath.substring(subLength,SchemaPath.length());
                JSONObject jsonObject = mapList.get(i).get(SchemaPath);
                Element schemaEl = serverElement.addElement("schema");
                if (!schema.isEmpty()){
                    Element schemaElCon = schemaEl.addAttribute("name", schema);
                    if (jsonObject.has("checkSQLSchema"))
                        schemaElCon.addAttribute("checkSQLschema",jsonObject.get("checkSQLSchema").toString());
                    if (jsonObject.has("defaultMaxLimit"))
                        schemaElCon.addAttribute("sqlMaxLimit",jsonObject.get("defaultMaxLimit").toString());
                    if (jsonObject.has("dataNode"))
                        schemaElCon.addAttribute("dataNode",jsonObject.get("dataNode").toString());
                    //处理 table
                    for (int j=0;j<mapList.size();j++) {
                        String tablePath = mapList.get(j).keySet().toString().replace("[", "").replace("]", "").trim();
                        if (!tablePath.contains(schema)){
                            continue;
                        }
                        String temp = tablePath.substring(subLength, tablePath.length());
                        if (temp.contains("/") && temp.contains(schema)&&temp.lastIndexOf("/")<=schema.length()) {
                            String tableName = temp.substring(schema.length() + 1, temp.length());
//                            System.out.println("table:" + tableName);
                            JSONObject tableJsonObject = mapList.get(j).get(tablePath);
                            Element tableEl = schemaEl.addElement("table");
                            if (tableJsonObject.has("name"))
                                tableEl.addAttribute("name", tableJsonObject.get("name").toString());
                            if (tableJsonObject.has("primaryschema"))
                                tableEl.addAttribute("primaryschema", tableJsonObject.get("primaryschema").toString());
                            if (tableJsonObject.has("datanode"))
                                tableEl.addAttribute("dataNode", tableJsonObject.get("datanode").toString());
                            if (tableJsonObject.has("type"))
                                if (tableJsonObject.get("type").toString().startsWith("1")) //目前只有1 全局表
                                tableEl.addAttribute("type", "global");
                            if (tableJsonObject.has("ruleName"))
                                tableEl.addAttribute("rule", tableJsonObject.get("ruleName").toString());
                            //处理childTable
                            for (int k=0;k<mapList.size();k++) {
                                String childTablePath = mapList.get(k).keySet().toString().replace("[", "").replace("]", "").trim();
                                if (!childTablePath.contains(schema)){
                                    continue;
                                }
                                if (childTablePath.equals(tablePath)||childTablePath.compareTo(tablePath)<=0)
                                    continue;
                                String tempChildTableName = childTablePath.substring(tablePath.length()-tableName.length(), childTablePath.length());
                                if (tempChildTableName.contains("/") && tempChildTableName.contains(tableName)) {
                                    String childTable = tempChildTableName.substring(tableName.length() + 1, tempChildTableName.length());
                                    if (tempChildTableName.substring(tempChildTableName.lastIndexOf("/")+1,tempChildTableName.length()).equals(childTable)){
//                                        System.out.println("childTable:" + childTable);
                                        JSONObject childTableJsonObject = mapList.get(k).get(childTablePath);
                                        Element childTableEl = tableEl.addElement("childTable");
                                        if (childTableJsonObject.has("name"))
                                            childTableEl.addAttribute("name", childTableJsonObject.get("name").toString());
                                        if (childTableJsonObject.has("primarykey"))
                                            childTableEl.addAttribute("primaryKey", childTableJsonObject.get("primarykey").toString());
                                        if (childTableJsonObject.has("parentkey"))
                                            childTableEl.addAttribute("parentKey", childTableJsonObject.get("parentkey").toString());
                                        if (childTableJsonObject.has("joinkey"))
                                            childTableEl.addAttribute("joinKey", childTableJsonObject.get("joinkey").toString());
                                        //处理 child-childTable
                                        for (int l=0;l<mapList.size();l++) {
                                            String child_childTablePath = mapList.get(l).keySet().toString().replace("[", "").replace("]", "").trim();
                                            if (!child_childTablePath.contains(schema)){
                                                continue;
                                            }
                                            if (child_childTablePath.equals(childTablePath)||child_childTablePath.compareTo(childTablePath)<=0||!child_childTablePath.contains(childTable))
                                                continue;
                                            String tempchild_childTablePath = child_childTablePath.substring(subLength+schema.length()+tableName.length()+2, child_childTablePath.length());
                                            if (tempchild_childTablePath.contains("/") && tempchild_childTablePath.contains(childTable)) {
                                                String child_childTablePathName = tempchild_childTablePath.substring(childTable.length() + 1, tempchild_childTablePath.length());
                                                if (tempchild_childTablePath.substring(tempchild_childTablePath.lastIndexOf("/")+1,tempchild_childTablePath.length()).equals(child_childTablePathName)){
                                                    //System.out.println("child-childTable:" + child_childTablePathName);
                                                    JSONObject child_childTableJsonObject = mapList.get(l).get(child_childTablePath);
                                                    Element child_childTablePathEl = childTableEl.addElement("childTable");
                                                    if (child_childTableJsonObject.has("name"))
                                                        child_childTablePathEl.addAttribute("name", child_childTableJsonObject.get("name").toString());
                                                    if (child_childTableJsonObject.has("primarykey"))
                                                        child_childTablePathEl.addAttribute("primaryKey", child_childTableJsonObject.get("primarykey").toString());
                                                    if (child_childTableJsonObject.has("parentkey"))
                                                        child_childTablePathEl.addAttribute("parentKey", child_childTableJsonObject.get("parentkey").toString());
                                                    if (child_childTableJsonObject.has("joinkey"))
                                                        child_childTablePathEl.addAttribute("joinKey", child_childTableJsonObject.get("joinkey").toString());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }}}
        }
        //datanode
        List<Map<String,JSONObject>> listDataNode = getDatanodeConfig(DATANODE_CONFIG_DIRECTORY);
        processDataNodeDocument(serverElement,listDataNode);

        //datahost
        List<Map<String,JSONObject>> listDataHost = getDataHostNodeConfig(CLU_PARENT_PATH,DATAHOST_CONFIG_DIRECTORY);
        processMysqlRepDocument(serverElement, listDataHost);
        json2XmlFile(document,"schema.xml");
    }


    //Datahost.xml
    public static void processDatahostDocument(Element serverElement,List<Map<String,JSONObject>> mapList){
        for (int i=0;i<mapList.size();i++){
        int subLength = CLU_PARENT_PATH.length()+DATAHOST_CONFIG_DIRECTORY.length()+2;
        String datahostName = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
        if (!datahostName.substring(subLength,datahostName.length()).contains("/")){
            String key =datahostName.substring(subLength,datahostName.length());
            JSONObject jsonObject = mapList.get(i).get(datahostName);
            Element dataHost = serverElement.addElement("dataHost");
            if (!key.isEmpty()){
                Element datahost = dataHost.addAttribute("name", key);
                if (jsonObject.has("writetype"))
                datahost.addAttribute("writeType",jsonObject.get("writetype").toString());
                if (jsonObject.has("switchType"))
                    datahost.addAttribute("switchType",jsonObject.get("switchType").toString());
                if (jsonObject.has("slaveThreshold"))
                    datahost.addAttribute("slaveThreshold",jsonObject.get("slaveThreshold").toString());
                if (jsonObject.has("balance"))
                    datahost.addAttribute("balance",jsonObject.get("balance").toString());
                if (jsonObject.has("dbtype"))
                    datahost.addAttribute("dbType",jsonObject.get("dbtype").toString());
                if (jsonObject.has("maxcon"))
                    datahost.addAttribute("maxCon",jsonObject.get("maxcon").toString());
                if (jsonObject.has("mincon"))
                    datahost.addAttribute("minCon",jsonObject.get("mincon").toString());
                if (jsonObject.has("dbDriver"))
                    datahost.addAttribute("dbDriver",jsonObject.get("dbDriver").toString());
                if (jsonObject.has("heartbeatSQL")){
                    Element  heartbeatSQL = dataHost.addElement("heartbeat");
                    heartbeatSQL.setText(jsonObject.get("heartbeatSQL").toString());
                }
                //处理WriteHost
                for (int j=0;j<mapList.size();j++) {
                    String host = mapList.get(j).keySet().toString().replace("[", "").replace("]", "").trim();
                    String temp = host.substring(subLength, host.length());
                    if (temp.contains("/") && temp.contains(key)&&temp.lastIndexOf("/")<=key.length()) {
                        String childHost = temp.substring(key.length() + 1, temp.length());
                        //System.out.println("childHost:" + childHost);
                        JSONObject childJsonObject = mapList.get(j).get(host);
                        Element writeHost = dataHost.addElement("writeHost");
                        if (childJsonObject.has("host"))
                            writeHost.addAttribute("host", childJsonObject.get("host").toString());
                        if (childJsonObject.has("url"))
                            writeHost.addAttribute("url", childJsonObject.get("url").toString());
                        if (childJsonObject.has("user"))
                            writeHost.addAttribute("user", childJsonObject.get("user").toString());
                        if (childJsonObject.has("password"))
                            writeHost.addAttribute("password", childJsonObject.get("password").toString());
                        //处理readHost
                        for (int k=0;k<mapList.size();k++) {
                            String readhost = mapList.get(k).keySet().toString().replace("[", "").replace("]", "").trim();
                            if (readhost.equals(host)||readhost.compareTo(host)<=0)
                                continue;
                            String tempread = readhost.substring(host.length()-childHost.length(), readhost.length());
                            if (tempread.contains("/") && tempread.contains(childHost)) {
                                String readHost = tempread.substring(childHost.length() + 1, tempread.length());
                                //System.out.println("readHost:" + readHost);
                                JSONObject readJsonObject = mapList.get(k).get(readhost);
                                Element readHostEl = writeHost.addElement("readHost");
                                if (readJsonObject.has("host"))
                                    readHostEl.addAttribute("host", readJsonObject.get("host").toString());
                                if (readJsonObject.has("url"))
                                    readHostEl.addAttribute("url", readJsonObject.get("url").toString());
                                if (readJsonObject.has("user"))
                                    readHostEl.addAttribute("user", readJsonObject.get("user").toString());
                                if (readJsonObject.has("password"))
                                    readHostEl.addAttribute("password", readJsonObject.get("password").toString());
                            }
                        }
                    }
                }
            }
        }
        }
    }

    //Datanode xml
    public static void processDataNodeDocument(Element serverElement,List<Map<String,JSONObject>> mapList){
        for (int i=0;i<mapList.size();i++){
            String dataNode = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
            JSONObject jsonObject = mapList.get(i).get(dataNode);
            Element dataNodeEl = serverElement.addElement("dataNode");
            if (jsonObject.has("name"))
                dataNodeEl.addAttribute("name", jsonObject.get("name").toString());
            if (jsonObject.has("dataHost"))
                dataNodeEl.addAttribute("dataHost", jsonObject.get("dataHost").toString());
            if (jsonObject.has("database"))
                dataNodeEl.addAttribute("database", jsonObject.get("database").toString());
            }
    }

    public static boolean json2XmlFile(Document document, String filename) {
        boolean flag = true;
        File file = new File(downloadDir, filename);
        if (file.isFile()) {
            log.warn("'{}' is existing in directory '{}'", filename, downloadDir);
            return false;
        }

        OutputStream out = null;
        try {
            /* 将document中的内容写入文件中 */
            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setEncoding(SystemConfig.CHARSET);
            out = new FileOutputStream(file);
            XMLWriter writer = new XMLWriter(out, format);
            writer.write(document);
            writer.close();
        } catch(Exception ex) {
            log.warn("Write xml file error: "+ file, ex);
            flag = false;
        } finally {
            IoUtil.close(out);
        }
        return flag;
    }

    @SuppressWarnings("unchecked")
	private static Map<String, Object> loadZkConfig() {
        InputStream configIS = SystemConfig.getConfigFileStream(ZK_CONFIG_FILE_NAME);
        try {
            return (Map<String, Object>) new Yaml().load(configIS);
        } finally {
            IoUtil.close(configIS);
        }
    }

    private static CuratorFramework createConnection(String url) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .newClient(url, new ExponentialBackoffRetry(100, 6));

        //start connection
        curatorFramework.start();
        //wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework;
            }
        } catch (InterruptedException e) {
        }

        //fail situation
        curatorFramework.close();
        throw new RuntimeException("failed to connect to zookeeper service: " + url);
    }

}
