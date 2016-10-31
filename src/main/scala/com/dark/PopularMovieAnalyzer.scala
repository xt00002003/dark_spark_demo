package com.dark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by darkxue on 19/10/16.
  */
object PopularMovieAnalyzer {

  def main(args: Array[String]): Unit = {
    val dataPath="/data/workspaces/test/dark_spark_demo/data/ml-1m/";
     val conf=new SparkConf().setMaster("local").setAppName("PopularMovieAnalyzer")
     val sc=new SparkContext(conf)

    //step 1 : get male who's age is  form 18 to 24
    val usersRDD=sc.textFile(dataPath+"users.dat")
    val maleRDD=usersRDD.map(n=>{
       val array=n.split("::")
      (array(0),(array(1),array(2)))
    }).filter(user=>(user._2._2.toInt ==18 && user._2._1.equals("M")))

    //maleRDD.foreach(println)

    //step 2 :
    val ratingRDD=sc.textFile(dataPath+"ratings.dat")

    val ratingFilterRDD=ratingRDD.map(n=>{
       val array=n.split("::")
      (array(0),(array(1),array(2)))
    })

    val maleJoinRDD=maleRDD.join(ratingFilterRDD)

    //maleJoinRDD.foreach(println)


//    val result=maleJoinRDD.map(line=>{
//      (line._2._2._1,line._2._2._2.toInt)
//    }).reduceByKey(_ + _).sortBy(user=>user._2,false).take(10)

    val result=maleJoinRDD.map(line=>{
      (line._2._2._1,1)
    }).reduceByKey(_ + _).sortBy(user=>user._2,false).take(10)


    result.foreach(println)
  }

}
