package com.dark

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
    val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3308/dark", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "user_info", "user" -> "root", "password" -> "Aa123456")).load()
    jdbcDF.show()
  }
}
