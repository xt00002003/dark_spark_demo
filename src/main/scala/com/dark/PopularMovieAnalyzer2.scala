package com.dark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by darkxue on 19/10/16.
  */
object PopularMovieAnalyzer2 {
  def main(args: Array[String]): Unit = {
    val dataPath="/data/workspaces/test/dark_spark_demo/data/ml-1m/";
    val conf=new SparkConf().setMaster("local").setAppName("PopularMovieAnalyzer")
    val sc=new SparkContext(conf)

    //step 1 : get male who's age is  form 18 to 24
    val MALE_AGE="18"
    val MALE="M"
    val usersRDD=sc.textFile(dataPath+"users.dat")
//    usersRDD.map(_.split("::")).map{
//      line=>(line(0),(line(1),line(2)))
//    }.filter(_._2._2.equals(MALE_AGE)).filter(_._2._1.equals(MALE)).map(n=>n._1)

    val userIDArray=usersRDD.map(line=>{
       val array=line.split("::")
      (array(0),(array(1),array(2)))
    }).filter(_._2._2.equals(MALE_AGE)).filter(_._2._1.equals(MALE)).map(n=>n._1)
      .collect()

    //broadcast
    val userIDHashSet=mutable.HashSet() ++ userIDArray
    val userBroadcast=sc.broadcast(userIDHashSet)

    val ratingRDD=sc.textFile(dataPath+"ratings.dat")
    val ratingArray=ratingRDD.map(_.split("::")).map{
       line=>(line(0),line(1))
    }.filter(n=>{
       userBroadcast.value.contains(n._1)
    }).map(line=>(line._2,1))
      .reduceByKey(_ + _).sortBy(_._2,false).take(10)

    val moveiesRDD=sc.textFile(dataPath+"movies.dat")

    val movieNameArray=moveiesRDD.map(_.split("::")).map{
      line=>(line(0),line(1))
    }.collect().toMap

    ratingArray.map(x=>{
      (movieNameArray.getOrElse(x._1,null),x._2)
    }).foreach(println)
  }
}
