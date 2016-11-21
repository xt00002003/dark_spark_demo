package com.dark.channel.service


import java.util.UUID

import com.dark.channel.entity._
import com.dark.channel.util.{DateTimeUtils, EsClientFactory, JsonUtil, PropertiesUtil}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.{JSON, JSONObject}
import org.apache.spark.sql.{SQLContext, SaveMode}


/**
  * Created by dark on 2016/11/12.
  * 这个示例的目的就是尝试解析日志，把日志转化成spark sql能否解析的日志格式.
  * 根据spark课程中的介绍。spark sql 是未来的趋势而且性能要比一般直接使用rdd要好。所以
  * 尝试把改写成使用spark sql。
  * device_id的生成策略就是uuid
  */
object LoadData {

  def main(args: Array[String]): Unit = {

    val osname=System.getProperties().getProperty("os.name")

    val dataPath=getDataPath(osname)



//    val dataPath="D:\\IdeaProjects\\dark_spark_demo\\data\\json\\";
    val conf=new SparkConf().setMaster("local").setAppName("LoadData")
    val sc=new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val jsonData=sc.textFile(dataPath+"CheckData.1474300800001").cache()
    val preDdd=PrePraseLog.prePraseLog(jsonData).cache()

    /**
      * 统计启动次数
      */
//    val activitiesJsonData=jsonData.map(line=>{
//      converterActivitiesLog(line)
//
//    }).flatMap(_.split(";"))
//
//    val df=sqlContext.read.json(activitiesJsonData)
//    df.printSchema()
//    df.show()
//
//    import sqlContext.implicits._
//    val channelRdd=df
//    //按应用设备数去重
//    val processedChannelRdd=df.groupBy("pn","ch").max("timestamp").withColumnRenamed("max(timestamp)","timestamp").join(df,Seq("pn","ch","timestamp"),"left").show()


    //判断新用户使用的是没有去重复的数据。
    /**
      * AnalysisNewDevice
      * 经过筛选最后只剩下有时间最大的一条设备记录。用于统计新设备用户(app_device)。这样就避免了多个work同时插入新设备的问题。
      * 1.原先逻辑使用了mapToPair、aggregateByKey来进行数据的筛选。
      * 2.使用mapPartitionsToPair、reduceByKey来生成应用新增设备总数。
      * 如果是新设备就插入到es中app_device表。不是就删除这条记录。最后对剩下的记录统计应用的新设备数。
      * 3.最后使用foreachPartition来更新修改用户。
      */
//    val newDeviceRDD=jsonData.map(line=>{
//      val array=line.split("[|]")
//      val timeAndIp=Map("timestamp"->array(0).toLong,"ip"->array(1))
//      val jsonStr=array(2)
//      val jeroenMap = JsonUtil.fromJson[Map[String,Any]](jsonStr)
//      var channelDevice=jeroenMap("us").asInstanceOf[Map[String,Any]]
//      channelDevice ++= timeAndIp
//      JsonUtil.toJson(channelDevice)
//
//    })


    val newDeviceDF=sqlContext.read.json(preDdd)
    newDeviceDF.printSchema()
    newDeviceDF.show()
    val deviceDF=newDeviceDF.groupBy("deviceId").max("timestamp").withColumnRenamed("max(timestamp)","timestamp").join(newDeviceDF,Seq("deviceId","timestamp")).cache()
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
            val esId=pkgName+"#"+deviceId
            EsClientFactory.getEsTransportClient.prepareIndex("p_channel","app_devices",esId).setSource(source).execute().actionGet()

         }
      }
    )

    /**
      * 这里只是为了练习spark sql 而这样做了。这样实现的功能并不好，原因如下：
      *  1.最后的数据都集中返回给了driver上。数据量大的话，会把driver机器撑爆。
      *  2.查询和插入如果使用spark sql去操作的话反而性能不好。因为还要转化成rdd去执行。不如直接使用es的客户端去查询和操作。
      */
//    val data=deviceDF.collect()
//    for(row<-data) {
//      val im = row(0).asInstanceOf[String]
//      val mac = row(1).asInstanceOf[String]
//      val dateTime = row(2).asInstanceOf[Long]
//      val ai = row(3).asInstanceOf[String]
//
//
//
//      val uriQuery = "?q=_id:" + im
//      var finalDeviceId=""
//      val options = Map[String, String]("es.read.field.include" -> "username,password,mail,uuid", "es.query" -> uriQuery)
//      val data = sqlContext.read.format("org.elasticsearch.spark.sql").options(options).load("p_channel/devices")
//      val count = data.count()
//      if (count < 1) {
//        val addOptions = Map("es.write.operation" -> "index", "es.resource" -> "p_channel/devices", "es.mapping.id" -> "device_id")
//        val newDeviceId = UUID.randomUUID().toString
//        finalDeviceId=newDeviceId
//        val uuid = UUID.randomUUID().toString
//        val newDevice = NewDeviceInfo(newDeviceId, im, ai, uuid, mac, dateTime)
//
//        val array = Array(JsonUtil.toJson(newDevice))
//
//        import sqlContext.implicits._
//        val deviceDataFrame = sc.parallelize(array).map(line=>{
//            JsonUtil.fromJson[NewDeviceInfo](line)
//        }).toDF()
//        deviceDataFrame.printSchema()
//        deviceDataFrame.show()
//        deviceDataFrame.write.format("org.elasticsearch.spark.sql").options(addOptions).mode(SaveMode.Append).save()
//
//      }
//
//      val appKey=row(5).asInstanceOf[String]
//      val country=row(6).asInstanceOf[String]
//      val channel=row(8).asInstanceOf[String]
//      val ip=row(12).asInstanceOf[String]
//      val language=row(14).asInstanceOf[String]
//      val packName=row(16).asInstanceOf[String]
//      val model=row(18).asInstanceOf[String]
//      val deviceId=UUID.randomUUID().toString
//      val id=packName+"#"+deviceId
      //    }
      //    deviceDF.registerTempTable("newDeviveInfo")
      //    deviceDF.foreachPartition(iter=>{
      //       if (iter.hasNext){
      //         val row=iter.next()
      //         val im=row(0).asInstanceOf[String]
      //         val mac=row(1).asInstanceOf[String]
      //         val ai=row(3).asInstanceOf[String]
      //         val dateTime=row(9).asInstanceOf[String]
      //         val uriQuery="?q=_id:"+im
      //         val sqlContext = new SQLContext(sc)
      //         val options = Map[String,String]("es.read.field.include" -> "username,password,mail,uuid","es.query"->uriQuery)
      //         val data=sqlContext.read.format("org.elasticsearch.spark.sql").options(options).load("dark_test/person")
      //         val count=data.count()
      //       }

      //add device info
      //        if (count<1){
      //          val addOptions = Map("es.write.operation" -> "index", "es.resource" -> "dark_test/person","es.mapping.id"->"device_id")
      //          val deviceId=UUID.randomUUID().toString
      //          val uuid=UUID.randomUUID().toString
      //          val newDevice=NewDeviceInfo(deviceId,im,ai,uuid,mac,dateTime)
      //
      //          val array=Array(JsonUtil.toJson(newDevice))
      //
      //
      //          import sqlContext.implicits._
      //          val deviceDataFrame = sc.parallelize(array).toDF()
      //          deviceDataFrame.printSchema()
      //          deviceDataFrame.show()
      //          deviceDataFrame.write.format("org.elasticsearch.spark.sql").options(addOptions).mode(SaveMode.Append).save()
      //
      //        }
      //    })
//    }

  }

  def getDataPath(osName:String):String={
    osName match {
        case "Linux" => PropertiesUtil.getValue("linux.path")
        case "Windows 8.1" =>PropertiesUtil.getValue("windows.path")
        case _ => PropertiesUtil.getValue("linux.path")

    }
  }

  @deprecated
  def parseLog(map:Map[String, Any]):ChannelInfo={
    val device=map("us")
    val activities=map("se")
    val deviceMap=device.asInstanceOf[Map[String,Any]]

    val channelDeviceInfo=ChannelDeviceInfo(deviceMap("ai").asInstanceOf[String],
      deviceMap("am").asInstanceOf[Double],deviceMap("appkey").asInstanceOf[String],deviceMap("c").asInstanceOf[String],deviceMap("cc").asInstanceOf[Double],
      deviceMap("ch").asInstanceOf[String],deviceMap("dateTime").asInstanceOf[String],deviceMap("fr").asInstanceOf[Double],deviceMap("h").asInstanceOf[Double],
      deviceMap("im").asInstanceOf[String],deviceMap("ir").asInstanceOf[Boolean],deviceMap("la").asInstanceOf[String],deviceMap("mac").asInstanceOf[String],
      deviceMap("om").asInstanceOf[Double],deviceMap("pn").asInstanceOf[String],deviceMap("sv").asInstanceOf[Double],deviceMap("t").asInstanceOf[String],deviceMap("w").asInstanceOf[Double])

    val activitiesList=activities.asInstanceOf[List[Map[String,Double]]]
    var  list=new ArrayBuffer[ChannelActivitiesInfo];
    for(n<-activitiesList){
      val  map=n.asInstanceOf[Map[String,Double]]
      val channelActivitiesInfo=ChannelActivitiesInfo(map("e"),map("s"))
      list += channelActivitiesInfo;
    }

    ChannelInfo(channelDeviceInfo,list.toArray)
  }

  /**
    * 把原日志的json转换成spark sql 能够解析的格式。
    *
    * @param line
    * @return
    */
  def converterActivitiesLog(line:String):String={

    val array=line.split("[|]")
    val timeAndIp=Map("timestamp"->array(0).toLong,"ip"->array(1))
    val jsonStr=array(2)
    val jeroenMap = JsonUtil.fromJson[Map[String,Any]](jsonStr)
    val activities=jeroenMap("se").asInstanceOf[List[Map[String,Long]]]

    val resultStr=new StringBuilder();
    for(n<- activities){
      var channelDevice=jeroenMap("us").asInstanceOf[Map[String,Any]]
      channelDevice ++= timeAndIp
      channelDevice ++= n
      val tmp=JsonUtil.toJson(channelDevice)
      resultStr.append(tmp)
      resultStr.append(";")

    }
    resultStr.substring(0,resultStr.length-1).mkString
  }


}
