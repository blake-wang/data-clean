package com

import java.util.Date

import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{HasOffsetRanges, OffsetRange, TopicQueueId}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

/**
  * Created by Admin on 2017-08-04.
  */
object Deal {
  def process(rdd:RDD[MessageExt]): Unit ={

    val topic="hello"

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val result=rdd.foreachPartition { iter =>
        val queueId = TaskContext.get.partitionId
        val offsets: Array[OffsetRange] = offsetRanges.get(new TopicQueueId(topic, queueId))
        offsets.foreach(println)
        println("--------------------")
      }
//      .map(line=>{
//      val str=new String(line.getBody)
//      val words=str.split(" ")
//      if(words.length==3){
//        (words(2).toInt,1)
//      }else{
//        (-1,1)
//      }
//    }).reduceByKey(_+_).sortBy(_._1)
//    println("总数为：")
//    println(result.count())
//    result.collect().foreach(println)
//    println()
  }
}
