package com.dark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by darkxue on 25/10/16.
  */
object SparkSQLFirstProgrammer {
  def main(args: Array[String]): Unit = {
    val dataPath="/data/workspaces/test/dark_spark_demo/data/json/";
    val conf=new SparkConf().setMaster("local").setAppName("SparkSQLFirstProgrammer")
    val sc=new SparkContext(conf)
    val sqlContext=new  SQLContext(sc)
    val df=sqlContext.read.json(dataPath+"git.txt")
    df.show()
    df.printSchema()
    df.select("author").show()
    // 选择date 等于 Wed Dec 30 15:04:16 2015 +0800 的数据
    df.filter("date = 'Wed Dec 30 15:04:16 2015 +0800' ").show()
    df.select(df("author_email"),df("commit")).show()
    df.drop("author").show()

  }
}
