package com.dark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by darkxue on 14/10/16.
  */
object AccumulatorProgrammer {
  def main(args: Array[String]): Unit = {
     val appName="AccumulatorProgrammer"
     val conf=new SparkConf().setMaster("local").setAppName(appName)
     val sc=new SparkContext(conf)
     val totalCount=sc.accumulator(0L,"totalCount")
     val count1=sc.accumulator(0L,"count1")
     val count2=sc.accumulator(0L,"count2")
     val result=sc.parallelize(List(1,2,3),3).map(num=>{

     })

  }
}
