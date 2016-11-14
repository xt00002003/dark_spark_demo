package com.original.channel.conf;

import com.alibaba.fastjson.JSON;
import com.original.channel.util.DateTimeUtils;
import com.original.channel.util.DbUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.isNotEmpty;

/**
 * utils to create Spark Context
 */
public class ContextFactory {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ContextFactory.class);

    private static Configuration config;
    private static transient JavaSparkContext sc;
    private static transient SQLContext sql;
    private static transient HiveContext hive;
    private static Map<String, String> envMap;
    private static Map<String, Object> rddMap;
    private static Map<String,HashMap> channels; //配置的channel数据



    public static void createContext(String statDateStr) {
        if (null != config) {
            return;
        }
        try {
            config = new Configuration();
            final String appName = config.getString(Configuration.APP_NAME);
            final String master = config.getString(Configuration.SPARK_MASTER, true);
            final SparkConf conf = new SparkConf().setAppName(appName);
            if (isNotEmpty(master)) {
                conf.setMaster(master);
            }
            sc = new JavaSparkContext(conf);
            sql = new SQLContext(sc);
            hive = new HiveContext(sc);
            envMap = new HashMap<String, String>();
            final DateTime statDate = DateTimeUtils.fromDateString(statDateStr);
            envMap.put("statDateStr", statDateStr);
            envMap.put("statDateString", DateTimeUtils.toDateString(statDate));
            envMap.put("statEsTimeString", DateTimeUtils.toEsTimeString(statDate));
            rddMap = new HashMap<String, Object>();
            //初始化数据库连接
            String driver = config.getString(Configuration.DB_DRIVER);
            String url = config.getString(Configuration.DB_URL);
            String username = config.getString(Configuration.DB_USER);
            String password = config.getString(Configuration.DB_PWD);

            channels= DbUtil.getChanelList(DbUtil.getConnection(driver,url,username,password));
            LOG.info("数据库初始化渠道配置信息,{}", JSON.toJSONString(channels));
              /* Set Log Level */
            final String levelStr = config.getString(Configuration.LOG_LEVEL);
            sc.setLogLevel(levelStr);
            Logger.getLogger("org").setLevel(Level.WARN);
            Logger.getLogger("akka").setLevel(Level.WARN);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }



    public static Configuration getConfig() {
        return config;
    }

    public static JavaSparkContext getSparkContext() {
        return sc;
    }

    public static SQLContext getSQLContext() {
        return sql;
    }

    public static HiveContext getHiveContext() {
        return hive;
    }

    public static Map<String, String> getEnvMap() {
        return envMap;
    }

    public static Map<String, Object> getRddMap() {
        return rddMap;
    }

    public static Map<String, HashMap> getChannels() {
        return channels;
    }
}
