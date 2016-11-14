package com.original.channel.device.analysis;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.google.gson.JsonObject;
import com.original.channel.conf.Configuration;
import com.original.channel.conf.ContextFactory;
import com.original.channel.elasticsearch.EsJestSupport;
import com.original.channel.elasticsearch.EsOperation;
import com.original.channel.model.ChannelLogInfo;
import com.original.channel.util.Constants;
import com.original.channel.util.ConvertUtils;
import com.original.channel.util.DateTimeUtils;
import io.searchbox.client.JestClient;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * APP的次日、3日、7日、30日后的留存率分析
 *
 * @author codethink
 * @date 6/17/16 1:39 PM
 */
public class AnalysisKeepDevice extends Analysis {

    private static final Logger LOG = LoggerFactory.getLogger(AnalysisKeepDevice.class);

    @Override
    public void execute() {
        final JavaRDD<ChannelLogInfo> channelRdd =
            (JavaRDD<ChannelLogInfo>) ContextFactory.getRddMap().get("processedChannelRdd");
        analysisKeep(channelRdd);
    }



    /**
     * 统计渠道应用留存用户
     *
     * @param channelRdd
     */
    public void analysisKeep(final JavaRDD<ChannelLogInfo> channelRdd) {
        LOG.info("留存率分析=====================================================");
        //查找ES中昨日新增用户
        final Configuration config = ContextFactory.getConfig();
        final EsOperation esOperation = new EsJestSupport(config.getEsConfigMap());
        final Map<String, String> envMap = ContextFactory.getEnvMap();
//        DateTime statDate = DateTimeUtils.fromUtcDateString(envMap.get("statDateString"));
        DateTime statDate = DateTimeUtils.fromEsTimeString(DateTimeUtils.toEsTimeString(DateTimeUtils.fromDateString(envMap.get("statDateString"))));

//        DateTime day = DateTimeUtils.fromUtcDateString("2016-5-31");
        //昨日新增用户
        DateTime day1=statDate.minusDays(1);
        final Map<String, HashMap> deviceMap1 = getNewDevice(esOperation, day1);
        DateTime day3=statDate.minusDays(3);
        final Map<String, HashMap> deviceMap3 = getNewDevice(esOperation, day3);
        DateTime day7=statDate.minusDays(7);
        final Map<String, HashMap> deviceMap7 = getNewDevice(esOperation, day7);
        DateTime day30=statDate.minusDays(30);
        final Map<String, HashMap> deviceMap30 = getNewDevice(esOperation, day30);
        //分析结果
        final Map<String, HashMap<String, Long>> keepMap = new ConcurrentHashMap<String, HashMap<String, Long>>();
        channelRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<ChannelLogInfo>, String, HashMap<String, Long>>() {
            public Iterable<Tuple2<String, HashMap<String, Long>>> call(Iterator<ChannelLogInfo> channelLogInfoIterator) throws Exception {
                while (channelLogInfoIterator.hasNext()) {
                    ChannelLogInfo logInfo = channelLogInfoIterator.next();
                    String deviceId = logInfo.device_id();
//                    String projectId = logInfo.project_id();
                    String packetName = logInfo.pkg_name();
                    String appDeviceKey = Joiner.on("#").join(packetName,deviceId);
                    LOG.info("今日活跃appDevice:{}", appDeviceKey);
                    //昨日留存用户数
                    statisticKeep(deviceMap1, keepMap, appDeviceKey, Constants.FIELD.KEEP_1);
                    //3日后留存用户
                    statisticKeep(deviceMap3, keepMap, appDeviceKey, Constants.FIELD.KEEP_3);
                    //7日后留存用户
                    statisticKeep(deviceMap7, keepMap, appDeviceKey, Constants.FIELD.KEEP_7);
                    //30日留存用户
                    statisticKeep(deviceMap30, keepMap, appDeviceKey, Constants.FIELD.KEEP_30);
                }
                final List<Tuple2<String, HashMap<String, Long>>> keepDeviceList =
                    new ArrayList<Tuple2<String, HashMap<String, Long>>>();
                for (String acc : keepMap.keySet()) {
                    keepDeviceList.add(new Tuple2<String, HashMap<String, Long>>(acc, keepMap.get(acc)));
                }
                return keepDeviceList;
            }
        }).reduceByKey(new Function2<HashMap<String, Long>, HashMap<String, Long>, HashMap<String, Long>>() {
            public HashMap<String, Long> call(HashMap<String, Long> v1, HashMap<String, Long> v2) throws Exception {
                Long keep1 = ConvertUtils.toLong(v1.get(Constants.FIELD.KEEP_1)) + ConvertUtils.toLong(v2.get(Constants.FIELD.KEEP_1));
                Long keep3 = ConvertUtils.toLong(v1.get(Constants.FIELD.KEEP_3)) + ConvertUtils.toLong(v2.get(Constants.FIELD.KEEP_3));
                Long keep7 = ConvertUtils.toLong(v1.get(Constants.FIELD.KEEP_7)) + ConvertUtils.toLong(v2.get(Constants.FIELD.KEEP_7));
                Long keep30 = ConvertUtils.toLong(v1.get(Constants.FIELD.KEEP_30)) + ConvertUtils.toLong(v2.get(Constants.FIELD.KEEP_30));
                HashMap<String, Long> statMap = new HashMap<String, Long>();
                statMap.put(Constants.FIELD.KEEP_1, keep1);
                statMap.put(Constants.FIELD.KEEP_3, keep3);
                statMap.put(Constants.FIELD.KEEP_7, keep7);
                statMap.put(Constants.FIELD.KEEP_30, keep30);
                return statMap;
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, HashMap<String, Long>>>>() {
            public void call(Iterator<Tuple2<String, HashMap<String, Long>>> tuple2Iterator) throws Exception {
                final EsOperation esOperation = new EsJestSupport(config.getEsConfigMap());
                final JestClient client = esOperation.getJestClient();
                LOG.info("=========留存率汇总");
                //入库ES
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, HashMap<String, Long>> tuple2 = tuple2Iterator.next();
                    String acc = tuple2._1();
                    HashMap<String, Long> stat = tuple2._2();
                    LOG.info("========留存率分析:projectChannel:{},keepDeviceTotal:{}", acc, JSON.toJSONString(stat));
                    //统计时间
                    String statDate = envMap.get("statDateString");
                    DateTime dayTime = DateTimeUtils.fromUtcDateString(statDate);
                    //1日后留存，更新1天前数据
                    String beforeDay1 = DateTimeUtils.toDateString(dayTime.minusDays(1));
                    saveKeep(esOperation, acc, Constants.FIELD.KEEP_1, stat.get(Constants.FIELD.KEEP_1), beforeDay1);
                    //3日后留存，更新3天前数据
                    String beforeDay3 = DateTimeUtils.toDateString(dayTime.minusDays(3));
                    saveKeep(esOperation, acc, Constants.FIELD.KEEP_3, stat.get(Constants.FIELD.KEEP_3), beforeDay3);
                    //7日后留存，更新7天前数据
                    String beforeDay7 = DateTimeUtils.toDateString(dayTime.minusDays(7));
                    saveKeep(esOperation, acc, Constants.FIELD.KEEP_7, stat.get(Constants.FIELD.KEEP_7), beforeDay7);
                    //30填后留存，更新30填前数据
                    String beforeDay30 = DateTimeUtils.toDateString(dayTime.minusDays(30));
                    saveKeep(esOperation, acc, Constants.FIELD.KEEP_30, stat.get(Constants.FIELD.KEEP_30), beforeDay30);
                }
            }
        });
    }


    /**
     * ES中获取某日新增用户
     *
     * @param esOperation
     * @return
     */
    public Map<String, HashMap> getNewDevice(EsOperation esOperation, DateTime start) {
        //某天新用户
        List<HashMap> appDevices = esOperation.search(esOperation.getJestClient(), start);
        HashMap<String, HashMap> idMaps = new HashMap<String, HashMap>();
        if (!CollectionUtils.isEmpty(appDevices)) {
            for (HashMap map : appDevices) {
                Object id = map.get("id");
                if (id != null) {
                    idMaps.put(id.toString(), map);
                }
            }
        }
        LOG.info("{}新增设备数:{}", start, idMaps.size());
        return idMaps;
    }

    /**
     * 判断数据是否留存
     *
     * @param newDeviceMap
     * @param keepMap
     * @param appDeviceKey
     * @param field,       指标字段
     */
    public void statisticKeep(final Map<String, HashMap> newDeviceMap, Map<String, HashMap<String, Long>> keepMap, String appDeviceKey, String field) {
        //是留存用户
        if (newDeviceMap.containsKey(appDeviceKey)) {
            //ES中
            HashMap device = newDeviceMap.get(appDeviceKey);
//            String appId = device.get("app_key").toString();
            String packetName = device.get("pkg_name").toString();
            String chanel = device.get("channel").toString();
            String country = device.get("country").toString();
            String acc = Joiner.on("#").join(packetName, chanel, country);
            HashMap<String, Long> appKeepMap = keepMap.get(acc);
            if (appKeepMap == null) {
                appKeepMap = new HashMap();
                appKeepMap.put(field, new Long(1));
                keepMap.put(acc, appKeepMap);
            } else {
                Long value = appKeepMap.get(field);
                appKeepMap.put(field, value == null ? 1 : value + 1);
            }
            LOG.info("留存统计结果:{}", JSON.toJSONString(appKeepMap));
        }
    }

    /**
     * 将统计结果入库ES
     *
     * @param esOperation
     * @param acc
     * @param field
     * @param deviceCount
     */
    public void saveKeep(EsOperation esOperation, String acc, String field, Long deviceCount, String day) {
        String[] attr = acc.split("#");
//        String projectCode = attr[0];//应用
        String packetName = attr[0];//应用
        String channel = attr[1];//渠道
        String country = attr[2];//国家

        Map<Object, Object> keepDevices = new HashMap<Object, Object>();
        String esId = Joiner.on("#").join(day, packetName, channel, country);
        JsonObject sta = esOperation.getDocumentById(
            esOperation.getJestClient(), Constants.ES.CHANNEL_INDEX, Constants.ES.CHANNEL_TYPE_STATISTICS, esId);
        if (sta == null) {
            keepDevices.put("pkg_name", packetName);
            keepDevices.put("channel", channel);
            keepDevices.put("country", country);
            keepDevices.put("day", day);
        }
        if(sta!=null && null!=sta.get("news")){
            float news=sta.get("news").getAsFloat();
            if (0==news){
                news=1;
            }
            float keep;
            if (null!=deviceCount){
                keep=Float.valueOf(deviceCount.toString());
                NumberFormat numberFormat = NumberFormat.getInstance();
                numberFormat.setGroupingUsed(false);
                // 设置精确到小数点后2位
                numberFormat.setMaximumFractionDigits(2);
                String keepRatio=numberFormat.format(keep / news);
                keepDevices.put(field+"Ratio", keepRatio);
            }

        }
        keepDevices.put(field, deviceCount);
        
        esOperation.upsertDocument(
            esOperation.getJestClient(), Constants.ES.CHANNEL_INDEX, Constants.ES.CHANNEL_TYPE_STATISTICS, esId, keepDevices);
        LOG.info("留存率指标入库成功,指标:{},留存用户:{},时间:{}", field, deviceCount, day);
    }
}
