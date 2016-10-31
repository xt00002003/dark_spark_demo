package com.dark

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

/**
  * Created by darkxue on 14/10/16.
  */
object FirstProgrammer {
  def main(args: Array[String]): Unit = {
    val appName="wordCount"
    val conf = new SparkConf().setAppName(appName)
    conf.setMaster("local")
    conf.set("spark.app.name", "MyFirstProgram")
    conf.set("spark.yarn.queue","infrastructure")
    val sc=new SparkContext(conf)
    sc.parallelize(List(1,2,3),2)
    val slices=10
    val n = 1 * slices
    val result=sc.parallelize(1 to n ,slices).map(x=>{
       val a=Random.nextInt()*2 -1
       val b=Random.nextInt()*2 -1
       println("the arg a is :"+a)
       println("the arg b is :"+b)
       val z=a*a + b*b
      println("the a*a + b*b is:"+z)
       if(z >0) 1 else 0

    }).reduce(_ + _)

    println("the final result is :"+result)



  }


}
