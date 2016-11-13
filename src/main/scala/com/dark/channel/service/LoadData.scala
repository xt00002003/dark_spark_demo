package com.dark.channel.service

import java.util

import com.dark.channel.entity.{ChannelActivitiesInfo, ChannelDeviceInfo, ChannelInfo, NewChannelInfo}
import com.dark.channel.util.JsonUtil
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.{JSON, JSONObject}
import org.apache.spark.sql.SQLContext


/**
  * Created by dark on 2016/11/12.
  */
object LoadData {

  def main(args: Array[String]): Unit = {
    val dataPath="D:\\IdeaProjects\\dark_spark_demo\\data\\json\\";
    val conf=new SparkConf().setMaster("local").setAppName("LoadData")
    val sc=new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val jsonData=sc.textFile(dataPath+"CheckData.1474300800001").map(line=>{
      val array=line.split("[|]")
      val jsonStr=array(2)
      //学习的要点。如何解析json字符串
//      val b = JSON.parseFull(jsonStr)
//      b match {
//        // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
//        case Some(map: Map[String, Any]) => parseLog(map)
//        case None => println("Parsing failed")
//        case other => println("Unknown data structure: " + other)
//      }
      converterLog(jsonStr)

    }).flatMap(_.split(";"))

    val df=sqlContext.read.json(jsonData)
    df.printSchema()
    df.show()

  }

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
    * @param line
    * @return
    */
  def converterLog(line:String):String={
    val jeroenMap = JsonUtil.fromJson[Map[String,Any]](line)

    val activities=jeroenMap("se").asInstanceOf[List[Map[String,Long]]]

    val resultStr=new StringBuilder();
    for(n<- activities){
      var channelDevice=jeroenMap("us").asInstanceOf[Map[String,Any]]
      channelDevice ++= n
      val tmp=JsonUtil.toJson(channelDevice)
      resultStr.append(tmp)
      resultStr.append(";")

    }
    resultStr.substring(0,resultStr.length-1).mkString
  }

}
