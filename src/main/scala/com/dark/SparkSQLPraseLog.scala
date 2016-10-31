package com.dark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by darkxue on 25/10/16.
  */
object SparkSQLPraseLog {
  def main(args: Array[String]): Unit = {
    val dataPath="/data/workspaces/test/dark_spark_demo/data/json/";
    val conf=new SparkConf().setMaster("local").setAppName("SparkSQLFirstProgrammer")
    val sc=new SparkContext(conf)

    val jsonData=sc.textFile(dataPath+"CheckData.1474300800000").map(line=>{
       val array=line.split("[|]")
       array(2)
    })

    val sqlContext=new  SQLContext(sc)
    val df=sqlContext.read.json(jsonData)
    df.registerTempTable("datalog")
    val sql="select us.ai,us.am,us.appkey,us.c,us.cc,us.ch,us.dateTime,us.fr,us.h,us.im,us.ir,us.la,us.mac,us.om,us.pn,us.sv,us.t,us.w,se.e,se.s from datalog"
//    sqlContext.sql(sql).show()
    val ai="ddQ6bLC0TYXPd57Y"
    val seSql="select log.se.e ,log.se.s from datalog as log where log.us.ai='ddQ6bLC0TYXPd57Y'"
    sqlContext.sql(seSql).foreach(println)


  }
}
