package com

import org.apache.rocketmq.common.message.MessageExt
import org.apache.spark.rdd.RDD

/**
  * Created by Admin on 2017-08-04.
  */
object deal {
  def process(rdd:RDD[MessageExt]): Unit ={
    println("scala")
    val result=rdd.repartition(50).map(line=>{
      val str=new String(line.getBody)
      val words=str.split(" ")
      if(words.length==3){
        (words(2),1)
      }else{
        ("error",1)
      }
    }).reduceByKey(_+_)

    result.collect().foreach(println)
    println(result.count())
    println("scala")
  }
}
