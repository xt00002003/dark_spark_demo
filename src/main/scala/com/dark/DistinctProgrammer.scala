package com.dark

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by darkxue on 21/10/16.
  */
object DistinctProgrammer {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("DistinctProgrammer")
    val sc=new SparkContext(conf)
    val numberRDD=sc.parallelize(Array(1,2,3,4,6,1,3))

    println("the count is :"+numberRDD.count())

    println("after distinct the count is :"+numberRDD.distinct().count())

    //每个元素都是一样的
    val mapRDD1=numberRDD.map(n=>{
      (n,Random.nextInt())
    }).distinct().foreach(println)

    //第二个元素不一样
    val mapRDD2=numberRDD.map(n=>{
      (n,Random.nextInt())
    }).distinct().foreach(println)

    //第二个元素不一样
    val mapRDD3=numberRDD.map(n=>{
      (new Persion(1),n)
    }).distinct().foreach(println)

  }
}
