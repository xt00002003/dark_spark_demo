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
import com.original.channel.util.DateTimeUtils;
import io.searchbox.client.JestClient;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * 渠道新用户判断、应用新用户数统计
 *
 * @author codethink
 * @date 6/16/16 6:00 PM
 */
@SuppressWarnings("serial")
public class AnalysisNewDevice extends Analysis {

    private static final Logger LOG = LoggerFactory.getLogger(AnalysisNewDevice.class);

    @Override
    public void execute() {
        @SuppressWarnings("unchecked")
        final JavaRDD<ChannelLogInfo> sdkRdd =
                (JavaRDD<ChannelLogInfo>) ContextFactory.getRddMap().get("channelRdd");
        analysisProjectNewDevices(sdkRdd);
    }

    /**
     * 分析设备新用户数
     */
    private void analysisProjectNewDevices(final JavaRDD<ChannelLogInfo> sdkRdd) {

        final Configuration config = ContextFactory.getConfig();
        final Map<String, String> envMap = ContextFactory.getEnvMap();
        //数据库中获取渠道配置渠道信息
        @SuppressWarnings("rawtypes")
        final Map<String, HashMap> channelMap = ContextFactory.getChannels();

        LOG.info("数据库中获取渠道列表,{}", JSON.toJSONString(channelMap));
         //数据结构[device_id,pkg_name,log]
        final JavaPairRDD<String, Map<String, ChannelLogInfo>> projectRdd =
                sdkRdd.mapToPair(new PairFunction<ChannelLogInfo, String, ChannelLogInfo>() {
                    public Tuple2<String, ChannelLogInfo> call(final ChannelLogInfo channelLogInfo) throws Exception {
                        return new Tuple2<String, ChannelLogInfo>(channelLogInfo.device_id(), channelLogInfo);
                    }
                }).aggregateByKey(
                        new HashMap<String, ChannelLogInfo>(),
                        new Function2<Map<String, ChannelLogInfo>, ChannelLogInfo, Map<String, ChannelLogInfo>>() {
                            public Map<String, ChannelLogInfo> call(final Map<String, ChannelLogInfo> projectMap,
                                                                    final ChannelLogInfo channelLogInfo) throws Exception {
//                                projectMap.put(channelLogInfo.project_id(), channelLogInfo);
                            	    //修改为将时间最大的日志保存
                            	ChannelLogInfo oldLog =  (ChannelLogInfo)projectMap.get(channelLogInfo.pkg_name());
									if (null == oldLog) {
										projectMap.put(channelLogInfo.pkg_name(), channelLogInfo);
	
									} else {
	
										Long oldTimestamp = oldLog.timestamp();
										Long newTimestamp = channelLogInfo.timestamp();
										if (oldTimestamp < newTimestamp) {
											projectMap.put(channelLogInfo.pkg_name(), channelLogInfo);
										}
									}
                                
                                return projectMap;
                            }
                        },
                        new Function2<Map<String, ChannelLogInfo>, Map<String, ChannelLogInfo>, Map<String, ChannelLogInfo>>() {
                            public Map<String, ChannelLogInfo> call(final Map<String, ChannelLogInfo> preProjectList,
                                                                    final Map<String, ChannelLogInfo> aftProjectList) throws Exception {
//                                for (final String projectCode : aftProjectList.keySet()) {
//                                    ChannelLogInfo logInfo = aftProjectList.get(projectCode);
//                                    Long debutTime = logInfo.timestamp();
//                                    if (preProjectList.containsKey(projectCode)) {
//                                        ChannelLogInfo preLogInfo = preProjectList.get(projectCode);
//                                        Long preDubutTime = (preLogInfo == null) ? 0 : preLogInfo.timestamp();
//                                        if (debutTime < preDubutTime) {
//                                            preProjectList.put(projectCode, logInfo);
//                                        }
//                                    } else {
//                                        preProjectList.put(projectCode, logInfo);
//                                    }
//                                }
                                for (final String packecName : aftProjectList.keySet()) {
                                    ChannelLogInfo logInfo = aftProjectList.get(packecName);
                                    Long debutTime = logInfo.timestamp();
                                    if (preProjectList.containsKey(packecName)) {
                                        ChannelLogInfo preLogInfo = preProjectList.get(packecName);
                                        Long preDubutTime = (preLogInfo == null) ? 0 : preLogInfo.timestamp();
                                        if (debutTime < preDubutTime) {
                                            preProjectList.put(packecName, logInfo);
                                        }
                                    } else {
                                        preProjectList.put(packecName, logInfo);
                                    }
                                }
                                return preProjectList;
                            }
                        });

        LOG.info("=======分析新用户数");
        projectRdd.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Map<String, ChannelLogInfo>>>, String, Long>() {
            public Iterable<Tuple2<String, Long>> call(Iterator<Tuple2<String, Map<String, ChannelLogInfo>>> iterator) throws Exception {

                // 获取Es相关操作
                final EsOperation esOperation = new EsJestSupport(config.getEsConfigMap());
                final JestClient client = esOperation.getJestClient();
                //定义计算应用在渠道的新设备数
                final Map<String, Long> projectNewDevices = new HashMap<String, Long>();
                while (iterator.hasNext()) {

                    Tuple2<String, Map<String, ChannelLogInfo>> devices = iterator.next();
                    String deviceId = devices._1();//设备Id
                    Map<String, ChannelLogInfo> projectData = devices._2();//应用设备Map
                    Iterator<Map.Entry<String, ChannelLogInfo>> projectIt =
                            projectData.entrySet().iterator();
                    // 判断是否是新设备
                    while (projectIt.hasNext()) {
                        Map.Entry<String, ChannelLogInfo> projectEntry = projectIt.next();
//                        String projectCode = projectEntry.getKey();
                        String packetName = projectEntry.getKey();
                        ChannelLogInfo logInfo = projectEntry.getValue();
                        Long firstTime = logInfo.timestamp();
                            //修改了esId的标识
                        String esId = Joiner.on("#").join(packetName, deviceId);

                        LOG.info("====>分析应用新用户,device_id:{},project_id:{},first_time:{}", deviceId, packetName, firstTime);
                        	//查询条件要修改
                        JsonObject oldSource =
                                esOperation.getDocumentById(client, Constants.ES.CHANNEL_INDEX, Constants.ES.CHANNEL_TYPE_APP_DEVICE, esId);

                        final Map<Object, Object> newSource = new HashMap<Object, Object>();
                        if (oldSource == null) {
                            //新设备
                            LOG.debug("====>判断是新设备,device_id:{},app_key:{}", deviceId, packetName);
                            newSource.put("id", esId);
                            newSource.put("device_id", deviceId);
                            //app包名
                            newSource.put("pkg_name", logInfo.pkg_name());
                            newSource.put("app_key", logInfo.project_id());
                            newSource.put("model", logInfo.model());
                            newSource.put("language", logInfo.language());
                            newSource.put("country", logInfo.country());
                            newSource.put("channel", logInfo.channel_id());
                            newSource.put("first_time", DateTimeUtils.toEsTimeString(logInfo.timestamp()));

                            //插入新设备
                            esOperation.indexDocument(
                                    client, Constants.ES.CHANNEL_INDEX, Constants.ES.CHANNEL_TYPE_APP_DEVICE, esId, newSource);
                        } else {
                            //设备已存在该应用,应用新设备去除
                            LOG.debug("=====>判断非新设备,device_id:{},app_key:{}", deviceId, packetName);
                            projectIt.remove();
                        }
                    }
                    //判断应用新设备数据
                    Iterator<Map.Entry<String, ChannelLogInfo>> projectNew =
                            projectData.entrySet().iterator();
                    while (projectNew.hasNext()) {
                        Map.Entry<String, ChannelLogInfo> projectEntry = projectNew.next();
//                        String projectCode = projectEntry.getKey();
                        String packetName = projectEntry.getKey();
                        ChannelLogInfo logInfo = projectEntry.getValue();
                        String channel = logInfo.channel_id();//渠道
                        String country = logInfo.country();//国家
                        String  projectCode= logInfo.project_id(); //应用
                        String pcc =
                                Joiner.on("#").join(packetName, channel, country, projectCode);//包名#渠道#国家#应用
                        //统计应用新增设备
                        if (projectNewDevices.containsKey(pcc)) {
                            projectNewDevices.put(pcc, projectNewDevices.get(pcc) + 1);
                        } else {
                            projectNewDevices.put(pcc, new Long(1));
                        }
                        LOG.debug("分析应用新增设备总数:{},{}", pcc, projectNewDevices.get(pcc));
                    }
                }
                final List<Tuple2<String, Long>> newDeviceList =
                        new ArrayList<Tuple2<String, Long>>(projectNewDevices.size());
                for (final String pcc : projectNewDevices.keySet()) {
                    newDeviceList.add(new Tuple2<String, Long>(pcc, projectNewDevices.get(pcc)));
                }
                return newDeviceList;
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
            public void call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
                final EsOperation esOperation = new EsJestSupport(config.getEsConfigMap());
                final JestClient client = esOperation.getJestClient();
                //总用户汇总
//                Map<String, Long> totalMap = esOperation.aggregation(client, "app_key");
                Map<String, Long> totalMap = esOperation.aggregation(client, "pkg_name");

                LOG.info("聚合应用总用户数:{}", JSON.toJSONString(totalMap));

                LOG.info("=========应用新增用户汇总入库ES");
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Long> projectTuple = tuple2Iterator.next();
                    String pcc = projectTuple._1();
                    Long newDeviceTotal = projectTuple._2();
                    String[] attr = pcc.split("#");
//                    String projectCode = attr[0];//应用
                    String packetName = attr[0];//包名
                    String channel = attr[1];//渠道
                    String country = attr[2];//国家
                    String projectCode = attr[3]; //应用
                    LOG.info("=====================project#Channel#Country:{},newDeviceTotal:{}", pcc, newDeviceTotal);
                    Map<Object, Object> newDevicesSource = new HashMap<Object, Object>();
                    String esId =
                            Joiner.on("#").join(envMap.get("statDateString"), packetName, channel, country);
                    JsonObject sta = esOperation.getDocumentById(
                            client, Constants.ES.CHANNEL_INDEX, Constants.ES.CHANNEL_TYPE_STATISTICS, esId);
                    if (sta == null) {
                        newDevicesSource.put("app_key", projectCode);
                        newDevicesSource.put("channel", channel);
                        newDevicesSource.put("country", country);
                        newDevicesSource.put("pkg_name", packetName);
                        newDevicesSource.put("@timestamp", envMap.get("statEsTimeString"));
                        LOG.info("====channel配置数据:{}", JSON.toJSONString(channelMap));
                        //填充渠道信息
                        if (channelMap != null) {
                            @SuppressWarnings("rawtypes")
                            Map channelInfo = channelMap.get(channel);
                            if (channelInfo != null) {
                                Object type = channelInfo.get("type");
                                newDevicesSource.put("type", type == null ? "" : type);
                                Object agent = channelInfo.get("agent");
                                newDevicesSource.put("agent", agent == null ? "" : agent);
                                Object source = channelInfo.get("source");
                                newDevicesSource.put("source", source == null ? "" : source);
                                Object localAppId = channelInfo.get("app_id");
                                newDevicesSource.put("app_id", localAppId);
                            } else {
                                //没有注册的渠道，渠道类型为
                                newDevicesSource.put("type", "default");
                            }
                        }
                    }
                    newDevicesSource.put("news", newDeviceTotal);
                    //根据渠道值判断渠道类型
//                    Long total = totalMap.get(projectCode);
                    Long total = totalMap.get(packetName);
                    newDevicesSource.put("total", total == null ? 0 : total); //总用户数
                    newDevicesSource.put("create_time", DateTimeUtils.toEsTimeString(DateTime.now()));
                    esOperation.upsertDocument(
                            client, Constants.ES.CHANNEL_INDEX, Constants.ES.CHANNEL_TYPE_STATISTICS, esId, newDevicesSource);
                }
            }
        });
    }


}
