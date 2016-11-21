package com.dark.channel.service


import java.util.{Date, UUID}

import com.dark.channel.entity.NewDeviceInfo
import com.dark.channel.util.{DateTimeUtils, EsClientFactory, JsonUtil}
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.index.query.QueryBuilders


/**
  * Created by darkxue on 21/11/16.
  */
object PrePraseLog {

  def prePraseLog(rdd: RDD[String]): RDD[String] = {
    rdd.map(line => {
      val array = line.split("[|]")
      val timeAndIp = Map("timestamp" -> array(0).toLong, "ip" -> array(1))
      val jsonStr = array(2)
      val jeroenMap = JsonUtil.fromJson[Map[String, Any]](jsonStr)
      var channelDevice = jeroenMap("us").asInstanceOf[Map[String, Any]]
      val imei=channelDevice("im").asInstanceOf[String]
      val mac=channelDevice("mac").asInstanceOf[String]
      val ai=channelDevice("ai").asInstanceOf[String]
      val uuid=UUID.randomUUID().toString
      val pkgName=channelDevice("pn").asInstanceOf[String]
      val deviceId=getDeviceId(imei,mac,ai,uuid,timeAndIp("timestamp").asInstanceOf[Long])
      channelDevice ++= timeAndIp
      channelDevice ++= Map("deviceId"->deviceId)
      JsonUtil.toJson(channelDevice)

    })

  }


  private def getDeviceId(imei: String, mac: String, androidId: String, uuid: String,timestamp:Long): String = {

    val queryImei=QueryBuilders.boolQuery()
      .must(QueryBuilders.termQuery("imei", imei));
    var response: SearchResponse=EsClientFactory.getEsTransportClient.prepareSearch("p_channel").setTypes("devices").setQuery(queryImei).execute().actionGet()

    var hits=response.getHits.hits()
    var isShoot=false
    if (!hits.isEmpty){
      isShoot=true
    }else{
      val queryMac=QueryBuilders.boolQuery().must(QueryBuilders.termQuery("mac",mac))
      response=EsClientFactory.getEsTransportClient.prepareSearch("p_channel").setTypes("devices").setQuery(queryMac).execute().actionGet()
      hits=response.getHits.hits()
      if (!hits.isEmpty){
        isShoot=true
      }
    }
    var deviceId=""
    if(isShoot){
      val firstTime=DateTimeUtils.fromEsTimeString(hits(0).getSource().get("first_time").asInstanceOf[String])
      deviceId=hits(0).getSource().get("device_id").asInstanceOf[String]
      val newDeviceInfo=NewDeviceInfo(deviceId,imei,androidId,uuid,mac, DateTimeUtils.toEsTimeString(firstTime))
      val jsonSource=JsonUtil.toJson(newDeviceInfo)
      EsClientFactory.getEsTransportClient.prepareUpdate("p_channel","devices",deviceId).setDoc(jsonSource).execute().actionGet()
    } else{

       deviceId=UUID.randomUUID().toString
       val newDeviceInfo=NewDeviceInfo(deviceId,imei,androidId,uuid,mac,DateTimeUtils.toEsTimeString(timestamp))
       val jsonSource=JsonUtil.toJson(newDeviceInfo)
       EsClientFactory.getEsTransportClient.prepareIndex("p_channel","devices").setSource(jsonSource).setId(deviceId).execute().actionGet()
    }

    deviceId

  }
}
