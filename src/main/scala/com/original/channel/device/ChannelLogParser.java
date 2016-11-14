package com.original.channel.device;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.original.channel.conf.Configuration;
import com.original.channel.conf.ContextFactory;
import com.original.channel.elasticsearch.EsJestSupport;
import com.original.channel.elasticsearch.EsOperation;
import com.original.channel.model.ChannelLogInfo;
import com.original.channel.model.StartUpLogInfo;
import com.original.channel.util.Constants;
import com.original.channel.util.DateTimeUtils;
import com.original.channel.util.MapUtils;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang.StringUtils.*;

/**
 * parse sdk log
 */
public class ChannelLogParser implements FlatMapFunction<Text,ChannelLogInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelLogParser.class);


    /**
     * parse sdk log
     *
     * @param text log
     * @return sdk log info obj
     */
    public Iterable<ChannelLogInfo> call(final Text text) throws Exception {
        if (null == text || 0 == text.getLength()) {
            return emptyList();
        }
        final ChannelLogInfo sdk = parse(text.toString());
        if (null == sdk) {
            return emptyList();
        }
        return singletonList(sdk);
    }

    /**
     * parse device log
     *
     * @param line log
     * @return device info obj
     */
    private ChannelLogInfo parse(final String line) {
        try {
            final String[] data = split(line, "|", 3);
            if (null == data || 3 != data.length) {
				  LOG.error("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n {} \n", data.toString());
                throw new IllegalArgumentException("bad format sdk log");
            }
            JSONObject obj = null;
			try {
				obj = JSONObject.parseObject(data[2]);
			} catch (Exception e) {
				LOG.error("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n {} \n",data[2],e);
				throw new IllegalArgumentException("bad format sdk log");
			}

            if (null == obj) {
                throw new IllegalArgumentException("no data");
            }
            
            //如果日志体中的date字段不为空，使用该字段作为日志的timestamp
            if (!isEmpty(obj.getString("date"))) {
            	data[0] = obj.getString("date");
			}
            final ChannelLogInfo result = new ChannelLogInfo(Long.parseLong(data[0]), data[1]);
            final JSONObject us = obj.getJSONObject(Constants.SDK.US);

            if (null == us) {
                throw new IllegalArgumentException("User device info of log is null!");
            }
            
            // set project ID
            final String projectID = us.getString(Constants.US.APP_KEY);
            if (isEmpty(projectID)) {
                throw new IllegalArgumentException("appkey is missing");
            }
            result.project_id_$eq(projectID);
            //渠道
            String channel=us.getString(Constants.US.CHANNEL);
            if (!isEmpty(channel)){
                //如果channel不在我们监控channel里面则赋值default
                if (null== ContextFactory.getChannels().get(channel)){
                    result.channel_id_$eq("default");
                }else {
                    result.channel_id_$eq(channel);
                }

            }else{
            	//渠道字段为空，default
                result.channel_id_$eq("default");
            }
            //手机型号
            String model=us.getString(Constants.US.MODEL);
            if (!isEmpty(model)){
                result.model_$eq(model);
            }
            //国家
            String country=us.getString(Constants.US.COUNTRY);
            if (!isEmpty(country)){
                result.country_$eq(country);
            }else{
                result.country_$eq("");
            }
            //语言
            String language=us.getString(Constants.US.LANGUAGE);
            if (!isEmpty(language)){
                result.language_$eq(language);
            }
            //包名
            String pkgName=us.getString(Constants.US.PACKAGE);

            if (!isEmpty(pkgName)){
                result.pkg_name_$eq(pkgName);
            }else{
                result.pkg_name_$eq("");
            }

//            if (!isEmpty(channel)){
//                result.channel_id_$eq(channel);
//            }else{
//            	//渠道字段为空，标示为defualt
//                result.channel_id_$eq("defualt");
//            }
            
            //启动次数
            final JSONArray startEvents = obj.getJSONArray(Constants.SDK.SE);

            if (null != startEvents && startEvents.size() > 0) {
                final List<StartUpLogInfo> startList = new ArrayList<StartUpLogInfo>(startEvents.size());
                for (int i = 0; i < startEvents.size(); i++) {
                    final JSONObject eventObj = startEvents.getJSONObject(i);
                    Long startTime = parseLong(eventObj.getString("s"), -1L);
                    Long endTime = parseLong(eventObj.getString("e"), -1L);
                    StartUpLogInfo startInfo=new StartUpLogInfo(startTime,endTime);
                    startList.add(startInfo);
                }
                result.startup_list_$eq(startList.toArray(new StartUpLogInfo[startList.size()]));
            }
            
          //imei
          String imei =  us.getString("im");
			if (isEmpty(imei) || imei.equals("000000000000000") || imei.length() != 15) {
				imei = "";
			}
			result.imei_$eq(imei);

          //android_id
          String android_id =  us.getString("ai");
			if (isEmpty(android_id)) {
				android_id="";
			}
			result.android_id_$eq(android_id);
				
          //mac
          String mac =  us.getString("mac");
			if (isEmpty(mac)) {
				mac="";
			}
			result.mac_$eq(mac);

          //uuid
          String uuid =  us.getString("uuid");
			if (isEmpty(uuid)) {
				uuid="";
			}
			result.uuid_$eq(uuid);
			
	        //最后获取设备ID，set the device ID
	        final String deviceId = this.getDeviceId(result);
	        if (isEmpty(deviceId)) {
	            throw new IllegalArgumentException("SETTINGS_DEVICE_IDENTIFIER is missing");
	        }
	        result.device_id_$eq(deviceId);
            
          return result;
        } catch (final Exception e) {
            LOG.error("fail to parse sdk log", e);
            return null;
        }
    }

//    /**
//     * 获取定义的设备ID
//     * @param us
//     * @return
//     */
//    private String getDeviceId(final JSONObject us) {
//        final String imei = us.getString(Constants.US.IMEI);
//        if (!isEmpty(imei)) {
//            return imei;
//        }
//        final String mac = us.getString(Constants.US.MAC);
//        if (!isEmpty(imei)) {
//            return mac;
//        }
//        final String andoridId = us.getString(Constants.US.ANDROID_ID);
//        if (!isEmpty(andoridId)) {
//            return andoridId;
//        }
//        return null;
//    }
    
	/**
	 * 生成设备查询条件
	 * 
	 * @param pkg_name
	 * @param imei
	 * @param mac
	 * @param android_id
	 * @param uuid
	 * @return
	 */
    private String genDevQuery(String pkg_name,String imei,String mac,String android_id,String uuid){
    	
        String query = "{\n" +
                "        \"size\": 2,\n" +
                "        \"query\" : {\n" +
                "          \"function_score\" : {\n" +
                "              \"functions\":[\n" +
                "                {\n" +
                "                    \"filter\":{\"term\":{\"imei\":\"" + imei + "\"}},\n" +
                "                    \"weight\":8\n" +
                "                },\n" +
                "                {\n" +
                "                    \"filter\":{\"term\":{\"mac\":\"" + mac + "\"}},\n" +
                "                    \"weight\":4\n" +
                "                },\n" +
                "                {\n" +
                "                    \"filter\":{\"term\":{\"android_id\":\"" + android_id + "\"}},\n" +
                "                    \"weight\":2\n" +
                "                },\n" +
                "                {\n" +
                "                    \"filter\":{\"term\":{\"uuid\":\"" + uuid + "\"}},\n" +
                "                    \"weight\":1.5\n" +
                "                }\n" +
                "              ],\n" +
                "              \"score_mode\": \"sum\",\n" +
                "              \"boost_mode\":\"replace\"\n" +
                "          }\n" +
                "          }\n" +
                "    }";
        return query;
    	
    }
    
    /**
     * 获取定义的设备ID
     * @param logInfo
     * @return
     * @throws IOException 
     */
	private String getDeviceId(ChannelLogInfo logInfo) throws IOException {
		// ES查询
		Configuration config = ContextFactory.getConfig();
		EsOperation esOperation = new EsJestSupport(config.getEsConfigMap());
		JestClient jestClient = esOperation.getJestClient();
		String imei = logInfo.imei();
		String uuid = logInfo.uuid();
		String android_id = logInfo.android_id();
		String mac = logInfo.mac();
		String pkg_name = logInfo.pkg_name();
		String query = genDevQuery(pkg_name, imei, mac, android_id, uuid);
		Search.Builder builder = new Search.Builder(query).addIndex(Constants.ES.CHANNEL_INDEX).addType(Constants.ES.CHANNEL_TYPE_DEVICES);
		SearchResult result = jestClient.execute(builder.build());

		// 结果分析
		JsonObject jsonObject = result.getJsonObject();
		JsonObject hitsObject = jsonObject.getAsJsonObject("hits");
		JsonArray hitArr = hitsObject.getAsJsonArray("hits");
		JsonElement max_score = hitsObject.get("max_score");
		//是否命中设备
		boolean isShoot = false;
		JsonObject hit;
		JsonObject source = new JsonObject();

		String devIdUuid = null;
		if (null != hitsObject && null != hitArr && hitArr.size() > 0 && !max_score.isJsonNull() 	&& max_score.getAsDouble() > 1.0) {
			hit = hitsObject.getAsJsonArray("hits").get(0).getAsJsonObject();
			source = hit.getAsJsonObject(Constants.ES.ES_SOURCE);
			if (max_score.getAsDouble() >= 8.0) {
				isShoot = true;
			} else if (max_score.getAsDouble() >= 4.0) {
				//日志中且表中都存在imei才认为是新设备
				if (!isBlank(imei) && null != source.get("imei")) {
					isShoot = false;
				}else{
					isShoot = true;
				}
				
			} else if (max_score.getAsDouble() >= 2) {
				
				//日志中且表中都存在imei才认为是新设备，或者日志中或表中都存在mac才认为没有命中设备，为新设备
				if (!isBlank(imei) && null != source.get("imei") || !isBlank(mac) && null != source.get("mac")) {
					isShoot = false;
				}else{
					isShoot = true;
				}
				
			} else if (max_score.getAsDouble() >= 1.5) {
				////日志中且表中都存在imei才认为是新设备，或者日志中或表中都存在mac才认为没有命中设备，为新设备
				if (!isBlank(imei) && null != source.get("imei") || !isBlank(mac) && null != source.get("mac") || !isBlank(android_id) && null != source.get("android_id")) {
					isShoot = false;
				}else{
					isShoot = true;
				}
			}
		}

		if (isShoot) {
			// 如果存在设备ID更新设备信息
			DateTime first_time = DateTimeUtils.fromEsTimeString(source.get("first_time").getAsString());
			devIdUuid = source.get("device_id").getAsString();
			//String esId = source.get("pkg_name").getAsString() + "#" + devIdUuid;
			String esId = devIdUuid;
			Map<Object, Object> map = new HashMap<Object, Object>();
			if (logInfo.timestamp() < first_time.getMillis()) {
				first_time = new DateTime(logInfo.timestamp());
				map.put("first_time", DateTimeUtils.toEsTimeString(first_time));

			}
			if (null == source.get("imei") && !isBlank(imei)) {
				map.put("imei", imei);
			}
			if (null == source.get("mac") && !isBlank(mac)) {
				map.put("mac", mac);
			}
			if (null == source.get("android_id") && !isBlank(android_id)) {
				map.put("android_id", android_id);
			}
			if (null == source.get("uuid") && !isBlank(uuid)) {
				map.put("uuid", uuid);
			}

			if (CollectionUtils.isNotEmpty(map.keySet())) {
				esOperation.upsertDocument(jestClient, Constants.ES.CHANNEL_INDEX, Constants.ES.CHANNEL_TYPE_DEVICES, esId, map);
			}

		} else {
			// 如果不存在该设备，插入新设备
			String device_id = UUID.randomUUID().toString().replace("-", "");
			devIdUuid = device_id;
			// String esId = pkg_name + "#" + device_id;
			String esId = device_id;
			Map<Object, Object> map = MapUtils.asMap("device_id", device_id);
			
			if (!isBlank(imei)) {
				map.put("imei", imei);
			}
			if (!isBlank(mac)) {
				map.put("mac", mac);
			}
			if (!isBlank(android_id)) {
				map.put("android_id", android_id);
			}
			if (!isBlank(uuid)) {
				map.put("uuid", uuid);
			}
			
			String first_time = DateTimeUtils.toEsTimeString(logInfo.timestamp());
			map.put("first_time", first_time);
			
			esOperation.upsertDocument(jestClient, Constants.ES.CHANNEL_INDEX, Constants.ES.CHANNEL_TYPE_DEVICES, esId, map);
			try {
				Thread.sleep(1200l);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return devIdUuid;
	}    

    private long parseLong(final String value, final long defaultValue) {
        try {
            return Long.valueOf(value);
        } catch (final NumberFormatException e) {
            return defaultValue;
        }
    }
}
