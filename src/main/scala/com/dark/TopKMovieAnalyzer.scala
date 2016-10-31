package com.dark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by darkxue on 20/10/16.
  */
object TopKMovieAnalyzer {
  def main(args: Array[String]): Unit = {
    val dataPath="/data/workspaces/test/dark_spark_demo/data/ml-1m/";
    val conf=new SparkConf().setMaster("local").setAppName("PopularMovieAnalyzer")
    val sc=new SparkContext(conf)

    val ratingRDD=sc.textFile(dataPath+"ratings.dat")
    val cacheRDD=ratingRDD.map(line=>{
       val array=line.split("::")
      (array(0),(array(1),array(2)))
    }).cache()

    val top10Movie=cacheRDD.map(x=>{
      (x._2._1,(x._2._2.toInt,1))
    }).reduceByKey((v1,v2)=>{
      (v1._1+v2._1,v1._2+v2._2)
    })
      .map(n=>{
      (n._1,n._2._1.toFloat / n._2._2.toFloat)
    })
      .sortBy(_._2,false).take(10)

    top10Movie.foreach(println)

    val topPop=cacheRDD.map(n=>{
      (n._1,1)
    }).reduceByKey(_ + _).sortBy(_._2,false).take(10)

    topPop.foreach(println)


    val userRDD=sc.textFile(dataPath+"users.dat")
    val userCache=userRDD.map(line=>{
       val array=line.split("::")
      (array(0),array(1))
    }).cache()

    val userFemaleArray=userCache.filter(line=>{
       line._2.equals("F")
    }).map(_._1).collect()

    val userFemaleIDBroaccast=sc.broadcast(userFemaleArray)

    val topFemale=cacheRDD.filter(n=>{
      userFemaleIDBroaccast.value.contains(n._1)
    }).map(n=>{
      (n._1,1)
    }).reduceByKey(_ + _).sortBy(_._2,false).take(10)

    topFemale.foreach(println)

    val userMaleArray=userCache.filter(line=>{
      line._2.equals("M")
    }).map(_._1).collect()

    val userMaleIDBroaccast=sc.broadcast(userMaleArray)

    val topMale=cacheRDD.filter(n=>{
      userMaleIDBroaccast.value.contains(n._1)
    }).map(n=>{
      (n._1,1)
    }).reduceByKey(_ + _).sortBy(_._2,false).take(10)

    topMale.foreach(println)

    sc.stop()
  }
}
