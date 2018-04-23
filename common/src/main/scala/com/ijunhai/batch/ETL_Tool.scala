package com.ijunhai.batch

import com.ijunhai.storage.redis.RedisSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017-10-29.
  */
object ETL_Tool {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    val sparkConf=new SparkConf()
      //.setMaster("local")
      .setAppName("OldLoginBatch")
    val sc=new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSinkCluster = sc.broadcast(RedisSink.apply())
    val paths=args(0)
    val rdd=sc.textFile(paths)
    val topic=args(1)
    Save2Kafka.saveDStreamSync(rdd,kafkaSinkBroadcast,topic,8,ETL_Tool.getClass.getName)
  }
}
