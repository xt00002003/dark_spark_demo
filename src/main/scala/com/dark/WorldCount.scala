package com.dark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap

/**
  * Created by darkxue on 14/10/16.
  */
object WorldCount {
  def main(args: Array[String]): Unit = {
     val conf=new SparkConf().setMaster("local").setAppName("world count")
     val sc=new SparkContext(conf)
     val rdd=sc.textFile("/data/workspaces/test/dark_spark_demo/src/main/scala/resource/world.txt",2)
     val result1=rdd.flatMap(line=>{
         println(line)
         line.split(" ")

     }).map(word=>(
       word,1
       )).reduceByKey(_ + _).collect()
     for(n<-result1){
        println(n)
        println("=============================")
     }

    //the difference from map  and flatmap
    val result=rdd.map(line=>{
      line.split(" ")
    }).map(word=>(
      word,1
      )).collect()
    for(n<-result){
          println(n._1)
          println("=============================")
    }


  }
}
