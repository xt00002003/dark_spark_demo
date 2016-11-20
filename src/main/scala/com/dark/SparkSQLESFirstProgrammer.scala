package com.dark

import com.dark.channel.entity.{UserInfo, UserInfo2}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by dark on 2016/11/20.
  * spaek sql 操作 elasticsearch 的示例
  * 配置文件的参数说明，参考：https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
  */
object SparkSQLESFirstProgrammer {
  def main(args: Array[String]): Unit = {

//    addInfo2ES()
//    addInfo2ESById()
    selectAllDataFromEs()
//    selectDataFromEsById()
  }

  /**
    * 插入的时候id是自动生成的
    */
  def addInfo2ES()={
    val conf=new SparkConf().setMaster("local").setAppName("SparkSQLESFirstProgrammer")
    val sc=new SparkContext(conf)
//
    val sqlContext=new  SQLContext(sc)
    val options = Map("es.index.auto.create" -> "true", "es.resource" -> "dark_test/person")
    val dataMap=Array[String]("dark;123;123@163.com","dark1;123;123@163.com","dark2;123;123@163.com")
    //循环1000次产生3000条数据
    for(i <- 1 to 1000){

      val rdd=sc.parallelize(dataMap).map(line=>line.split(";")).map(line=>UserInfo(line(0),line(1),line(2)))
      import sqlContext.implicits._
      val userDataFrame = rdd.toDF()
      userDataFrame.show()
      userDataFrame.write.format("org.elasticsearch.spark.sql").options(options).mode(SaveMode.Append).save()
    }


  }

  /**
    * 通过设置es.mapping.id 为哪个字段来手动设置id。
    * 注意：如果多添加一个字段作为id则会在数据中也会添加字段。
    * 暂时没有找到能直接设置id的方式。
    * 这个方式同时也可以更新id相同的数据
    */
  def addInfo2ESById()={
    val conf=new SparkConf().setMaster("local").setAppName("SparkSQLESFirstProgrammer")
    val sc=new SparkContext(conf)
    //
    val sqlContext=new  SQLContext(sc)
    val options = Map("es.write.operation" -> "index", "es.resource" -> "dark_test/person","es.mapping.id"->"username")


    val dataMap=Array[String]("dark222;123;1233455@163.com")
    val rdd=sc.parallelize(dataMap).map(line=>line.split(";")).map(line=>UserInfo(line(0),line(1),line(2)))
    import sqlContext.implicits._
    val userDataFrame = rdd.toDF()
    userDataFrame.show()
    userDataFrame.write.format("org.elasticsearch.spark.sql").options(options).mode(SaveMode.Append).save()

  }

  /**
    * 获取es中指定的字段和meta数据。meta数据是存放在Map中的
    * es如果没有设置size大小默认只会返回10条记录。
    * 在这里使用load方法按照测试的结果：count是总数。
    * 建议不要使用这样的方式加载数据,会把内存呈爆的.
    */
  def selectAllDataFromEs()={
    val conf=new SparkConf().setMaster("local").setAppName("SparkSQLESFirstProgrammer")
    val sc=new SparkContext(conf)
    //
    val sqlContext=new  SQLContext(sc)
    val options = Map("es.read.field.include" -> "username,password,mail","es.read.metadata"->"true")
    val data=sqlContext.read.format("org.elasticsearch.spark.sql").options(options).load("dark_test/person")
    println("加载的条数:"+data.count())
    data.printSchema()
    data.show()
    data.rdd.foreach(println(_))
  }

  /**
    * 这里根据id查询数据是使用了es的uri query 方式
    */
  def selectDataFromEsById()={
    val conf=new SparkConf().setMaster("local").setAppName("SparkSQLESFirstProgrammer")
    val sc=new SparkContext(conf)
    val sqlContext=new  SQLContext(sc)
    val options = Map("es.read.field.include" -> "username,password,mail,uuid","es.query"->"?q=_id:AViBAklKH_jb4nlq9PVX")
    val data=sqlContext.read.format("org.elasticsearch.spark.sql").options(options).load("dark_test/person")
    data.printSchema()
    data.show()
  }

}

