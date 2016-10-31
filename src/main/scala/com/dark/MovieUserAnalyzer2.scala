package com.dark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  * Created by darkxue on 18/10/16.
  */
object MovieUserAnalyzer2 {
  def main(args: Array[String]): Unit = {
      val dataPath="/data/workspaces/test/dark_spark_demo/data/ml-1m/";
      val conf=new SparkConf().setMaster("local").setAppName("MovieUserAnalyzer")
      val sc=new SparkContext(conf)
      val moveiesRDD=sc.textFile(dataPath+"movies.dat")



      //step 1: just know movie name is "Lord of the Rings, The (1978)" . get id from moveiesRDD
      val MOVIE_TITLE = "Lord of the Rings, The (1978)"
      val movieIdArray=moveiesRDD.map(n=>{
          val array=n.split("::")
        (array(0),array(1))
      }).filter(_._2.equals(MOVIE_TITLE)).collect()
      var movieId=""
      if(movieIdArray.length>0){
        movieId=movieIdArray(0)._1
        println("the movie id is :"+movieId)
      }

    //step 2: get ratingRDD  then  filter  data  finally  store  it  to  broadcast
    val ratingRDD=sc.textFile(dataPath+"ratings.dat")
    val filterRatingRDD=ratingRDD.map(line=>{
       val  array=line.split("::")
       (array(0),array(1))
     }).filter(_._2.equals(movieId)).map(n=>n._1).collect()

    //Remove duplicate
    val ratingSet=HashSet() ++ filterRatingRDD
    val ratingBroadcast=sc.broadcast(ratingSet)

    //get final  result .
    val usersRDD=sc.textFile(dataPath+"users.dat")
    usersRDD.map(line=>{
       val array=line.split("::")
      (array(0),(array(1),array(2)))
    }).filter(user=>{
      ratingBroadcast.value.contains(user._1)
    }).map(line=>{
      (line._2,1)
    }).reduceByKey(_ + _)
      .foreach(println)


    sc.stop()
  }

}
