package com.original.channel.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author codethink
 * @date 6/21/16 5:21 PM
 */
public class DbUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DbUtil.class);

    /**
     * 创建jdbc数据库连接
     *
     * @param driver
     * @param url
     * @param username
     * @param password
     * @return
     */
    public static Connection getConnection(String driver, String url, String username, String password) {
        Connection connection = null;
        try {
            LOG.info("开始建立数据库连接，driver:{},url:{},username;{},password:{}", driver, url, username, password);
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
            LOG.info("连接成功...'");
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            ;
        }
        return connection;
    }

    /**
     * mysql中获取渠道列表
     *
     * @return
     */
    public static Map<String, HashMap> getChanelList(Connection connection) {
        Map<String, HashMap> data = new HashMap();
        final String sql = "select app_id,tracker,type,agent,source from channel";
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                HashMap row = new HashMap();
                String channel = rs.getString("tracker");
                row.put("app_id", rs.getString("app_id"));
                row.put("channel", channel);
                row.put("type", rs.getString("type"));
                row.put("agent", rs.getString("agent"));
                row.put("source", rs.getString("source"));
                data.put(channel, row);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return data;
    }
}
