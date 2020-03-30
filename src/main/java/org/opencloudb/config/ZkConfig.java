package org.opencloudb.config;

import com.google.common.base.Strings;
import org.json.JSONObject;
import org.opencloudb.config.loader.zookeeper.ZookeeperLoader;
import org.opencloudb.config.loader.zookeeper.ZookeeperSaver;
import org.opencloudb.config.util.ConfigException;
import org.slf4j.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ZkConfig {

    private static final Logger log = LoggerFactory.getLogger(ZkConfig.class);

    private static final String ZK_CONFIG_FILE_NAME = "/myid.properties";

    private ZkConfig() {
    }

    public synchronized static ZkConfig instance() {
        return new ZkConfig();
    }

    public void initZk() {
        Properties pros = loadMyid();

        //disable load from zookeeper,use local file.
        if (pros == null) {
            log.info("use local configuration to startup");
            return;
        }
        
        try {
            JSONObject jsonObject = new ZookeeperLoader().loadConfig(pros);
            new ZookeeperSaver().saveConfig(jsonObject);
            log.info("use zookeeper configuration to startup");
        } catch (Exception e) {
            log.error("Fail to load configuration form zookeeper", e);
            // Note: here should throw exception instead of failing to
            // local configuration(may be outdated or error)
            throw new ConfigException("Fail to load configuration form zookeeper", e);
        }
    }

    public Properties loadMyid() {
        Properties pros = new Properties();

        try (InputStream configIS = ZookeeperLoader.class.getResourceAsStream(ZK_CONFIG_FILE_NAME)) {
            if (configIS == null) {
                //file is not exist, so ues local file.
                return null;
            }

            pros.load(configIS);
        } catch (IOException e) {
            throw new ConfigException("Can't find myid properties file: " + ZK_CONFIG_FILE_NAME, e);
        }

        if (Boolean.parseBoolean(pros.getProperty("loadZk"))) {
            //validate
            String zkURL = pros.getProperty("zkURL");
            String myid = pros.getProperty("myid");

            if (Strings.isNullOrEmpty(zkURL) || Strings.isNullOrEmpty(myid)) {
                throw new ConfigException("zkURL and myid must be not null or empty!");
            }

            return pros;
        }

        return null;
    }

}
