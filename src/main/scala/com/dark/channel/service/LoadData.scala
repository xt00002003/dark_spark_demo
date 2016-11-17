package com.dark.channel.service


import com.dark.channel.entity.{ChannelActivitiesInfo, ChannelDeviceInfo, ChannelInfo, NewChannelInfo}
import com.dark.channel.util.{JsonUtil, PropertiesUtil}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext


/**
  * Created by dark on 2016/11/12.
  * 这个示例的目的就是尝试解析渠道项目的日志，把日志转化成spark sql能否解析的日志格式.
  * 根据spark课程中的介绍。spark sql 是未来的趋势而且性能要比一般直接使用rdd要好。所以
  * 尝试把改写成使用spark sql。
  */
object LoadData {

  def main(args: Array[String]): Unit = {

    val osname=System.getProperties().getProperty("os.name")

    val dataPath=getDataPath(osname)



//    val dataPath="D:\\IdeaProjects\\dark_spark_demo\\data\\json\\";
    val conf=new SparkConf().setMaster("local").setAppName("LoadData")
    val sc=new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val jsonData=sc.textFile(dataPath+"CheckData.1474300800001").map(line=>{

      converterLog(line)

    }).flatMap(_.split(";"))

    val df=sqlContext.read.json(jsonData)
    df.printSchema()
    df.show()

    import sqlContext.implicits._
    val channelRdd=df
    //按应用设备数去重
    val processedChannelRdd=df.groupBy("pn","ch").max("timestamp").withColumnRenamed("max(timestamp)","timestamp").join(df,Seq("pn","ch","timestamp"),"left").show()

    //判断新用户使用的是没有去重复的数据。
    /**
      * AnalysisNewDevice
      * 经过筛选最后只剩下有时间最大的一条设备记录。用于统计新设备用户。这样就避免了多个work同时插入新设备的问题。
      * 1.原先逻辑使用了mapToPair、aggregateByKey来进行数据的筛选。
      * 2.使用mapPartitionsToPair、reduceByKey来生成应用新增设备总数。
      * 如果是新设备就插入到es中app_device表。不是就删除这条记录。最后对剩下的记录统计应用的新设备数。
      * 3.最后使用foreachPartition来更新修改用户。
      */



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
  def converterLog(line:String):String={

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
