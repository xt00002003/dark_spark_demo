package com.dark.channel.service

import com.dark.channel.entity.AppDeviceInfo
import com.dark.channel.util.{DateTimeUtils, EsClientFactory, JsonUtil, SpecificBasicDateTimeReader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
  * Created by darkxue on 28/11/16.
  */
object NewDevicePrase {
   /**
    * 对于每一个Partition使用java中操作es的方法去插入数据
    *
    */
    def praseLog(rdd: RDD[String],sqlContext: SQLContext):Unit={
      val newDeviceDF=sqlContext.read.json(rdd)
      val deviceDF=newDeviceDF.groupBy("deviceId").max("timestamp").withColumnRenamed("max(timestamp)","timestamp").join(newDeviceDF,Seq("deviceId","timestamp")).cache()
      deviceDF.show()


      deviceDF.foreachPartition(
        iter=>{
          if(iter.hasNext){
            val row=iter.next()
            val deviceId=row(0).asInstanceOf[String]
            val timestamp=row(1).asInstanceOf[Long]
            val androidId=row(2).asInstanceOf[String]
            val appKey=row(4).asInstanceOf[String]
            val country=row(5).asInstanceOf[String]
            val channel=row(7).asInstanceOf[String]
            val imei=row(11).asInstanceOf[String]
            val ip=row(12).asInstanceOf[String]
            val language=row(14).asInstanceOf[String]
            val mac=row(15).asInstanceOf[String]
            val pkgName=row(17).asInstanceOf[String]
            val model=row(19).asInstanceOf[String]
            val appDeviceInfo=AppDeviceInfo(pkgName,channel,country,deviceId,DateTimeUtils.toEsTimeString(timestamp),deviceId,language,model,appKey)
            val source=JsonUtil.toJson(appDeviceInfo)
            var esId=pkgName+"#"+deviceId
            var response=EsClientFactory.getEsTransportClient.prepareGet("p_channel","app_devices",esId).execute().actionGet()
            var hits=response.getSource
            if(hits.isEmpty){
              EsClientFactory.getEsTransportClient.prepareIndex("p_channel","app_devices",esId).setSource(source).execute().actionGet()
              esId= pkgName+"#"+channel+"#"+country+appKey
              response=EsClientFactory.getEsTransportClient.prepareGet("p_channel","channel_statistics",esId).execute().actionGet()
              hits=response.getSource
              if(hits.isEmpty){
                val channelStatisticsMap=Map("@timestamp"->DateTimeUtils.toEsTimeString(timestamp),
                  "actives"->0,"startAvg"->0,"agent"->"","app_id"->appKey,"app_key"->appKey,"channel"->channel,
                  "channel_id"->"","country"->country,"create_time"->DateTimeUtils.toEsTimeString(timestamp),
                  "day"->0,"id"->esId,"keep1"->0,"keep1Ratio"->0.0,"keep3"->0,"keep3Ratio"->0.0,
                  "keep30"->0,"keep30Ratio"->0.0,"keep7"->0,"keep7Ratio"->0.0,"news"->1,
                  "pkg_name"->pkgName,"source"->"","starts"->1,"type"->"")
                EsClientFactory.getEsTransportClient.prepareIndex("p_channel","channel_statistics",esId).setSource(JsonUtil.toJson(channelStatisticsMap)).execute().actionGet()
              }


            }

          }
        }
      )
    }

  /**
    * 使用spark sql 直接插入es。验证业务逻辑是否正确
    * @param rdd
    * @param sqlContext
    */
  def praseLog2(rdd: RDD[String],sqlContext: SQLContext):Unit={
    val addESIdRdd=rdd.map(line=>{
      var jeroenMap = JsonUtil.fromJson[Map[String, Any]](line)
      val pn=jeroenMap("pn")
      val deviceId=jeroenMap("deviceId")
      val id=pn+"#"+deviceId
      val timestamp=jeroenMap("timestamp").asInstanceOf[Long]
      jeroenMap ++= Map("id"->id)
      jeroenMap ++= Map("first_time"->DateTimeUtils.toEsTimeString(timestamp))
      JsonUtil.toJson(jeroenMap)
    })
    val newDeviceDF=sqlContext.read.json(addESIdRdd)
    val deviceDF=newDeviceDF.groupBy("deviceId").max("timestamp").withColumnRenamed("max(timestamp)","timestamp").join(newDeviceDF,Seq("deviceId","timestamp")).cache()
    //这里必须给出全部字段的值，不然执行后会把没有的字段删除掉
    val newDeviceESDF=deviceDF
      .select("first_time","appkey","ch","c","pn","deviceId","la","t","id")
      .withColumnRenamed("appkey","app_key")
      .withColumnRenamed("ch","channel")
      .withColumnRenamed("c","country")
      .withColumnRenamed("pn","pkg_name")
      .withColumnRenamed("deviceId","device_id")
      .withColumnRenamed("la","language")
      .withColumnRenamed("t","model")
    val options = Map("es.write.operation" -> "index", "es.resource" -> "p_channel/app_devices","es.mapping.id"->"id",ConfigurationOptions.ES_SERIALIZATION_READER_VALUE_CLASS -> classOf[SpecificBasicDateTimeReader].getCanonicalName)
    //完成了新增app设备的插入
    newDeviceESDF.write.format("org.elasticsearch.spark.sql").options(options).mode(SaveMode.Append).save()
  }
}
