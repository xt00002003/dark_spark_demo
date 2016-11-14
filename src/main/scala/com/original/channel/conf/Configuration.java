package com.original.channel.conf;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration for spark context
 */
@NotThreadSafe
public class Configuration implements Serializable {

    /**
     * log
     */
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

    /**
     * constants
     */
    public static final String LOG_LEVEL = "LogLevel";
    public static final String APP_NAME = "AppName";
    public static final String APP_CACHE_DIR = "AppCacheDir";
    public static final String SPARK_MASTER = "SparkMaster";
    public static final String HDFS_PATH = "HdfsPath";
    public static final String DB_DRIVER = "db_driver";
    public static final String DB_URL = "db_url";
    public static final String DB_USER = "db_user";
    public static final String DB_PWD = "db_pwd";

    private Map<String, String> esConfigMap;
    private Properties properties;

    public Configuration() {
        properties = new Properties();

        LOG.info("Loading configuration...");
        try {
            InputStream inputStream =
                getClass().getClassLoader().getResourceAsStream("device.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Fail to load configuration", e);
        }
    }

    public Map<String, String> getEsConfigMap() {
        if (null == esConfigMap) {
            esConfigMap = getSubMap("es.");
        }
        return esConfigMap;
    }

    private Map<String, String> getSubMap(String keyPrefix) {
        HashMap<String, String> result = new HashMap<String, String>();

        for (Object o : properties.keySet()) {
            String key = (String) o;
            if (StringUtils.startsWith(key, keyPrefix)) {
                result.put(key, properties.getProperty(key));
            }
        }
        return result;
    }

    public int getInt(String key) {
        return Integer.parseInt(getString(key, false));
    }

    public String getString(String key) {
        return getString(key, false);
    }

    public String getString(String key, boolean isEmptyValueOkay) {
        String value = properties.getProperty(key);
        if (value == null || value.equals("")) {
            if (isEmptyValueOkay) {
                LOG.warn("Empty value found for " + key + ".  Returning empty string.");
                value = "";
            } else {
                throw new IllegalArgumentException("Could not find config value for " + key);
            }
        }
        return value;
    }
}
