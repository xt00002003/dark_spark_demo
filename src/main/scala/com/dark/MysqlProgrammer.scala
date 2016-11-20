package com.dark

import com.dark.channel.util.JsonUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dark on 2016/11/14.
  */
object MysqlProgrammer {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("MysqlProgrammer")
    val sc=new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

  }

  def selectDataFromMysql(sqlContext:SQLContext)={
    val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3308/dark", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "user_info", "user" -> "root", "password" -> "Aa123456")).load()
    jdbcDF.filter("user_id=1").rdd.foreach(println(_))
  }

  def addUserInfo2Mysql(sqlContext:SQLContext)={
    val dataMap=Map("username"->"dark","email"->"123@163.com","address"->"123abc")
    val dataJson=JsonUtil.toJson(dataMap)
    println(dataJson)
    val person=sqlContext.read.json(dataJson).toDF()
    val mysql_url="jdbc:mysql://localhost:3308/dark?user=root&password=Aa123456"
//    person.write.jdbc("jdbc:mysql://localhost:3308/dark?user=root&password=Aa123456","user_info",)
  }
}
