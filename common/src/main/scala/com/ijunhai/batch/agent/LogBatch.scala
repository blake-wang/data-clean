package com.ijunhai.batch.agent

import com.ijunhai.batch.process.LogBatchProcess
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object LogBatch {
  def main(args: Array[String]) {
    val service = args(0) //agent
    val useRedis = if (args(1) == "true") true else false //true 写入redis
    val sourceFilePath = args(2) //hdfs
    val destFilePath = if (args(3) == null) "" else args(3) //不写入hdfs则""

    val sparkConf = new SparkConf().setAppName("LogBatch")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[redis.clients.jedis.Jedis]))
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Connection]))


    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    val sc = new SparkContext(sparkConf)
    //    val map = mutable.Map(
    //      HBASESINK -> sc.broadcast(HbaseSink.apply()),
    //      KAFKASINK -> sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    //    )
    //    if (useRedis) {
    //      map.put(REDISSINK, sc.broadcast(RedisSink.apply()))
    //    }
    val hbaseSink = sc.broadcast(HbaseSink.apply())
    val kafkaSink = sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSink = sc.broadcast(RedisSink.apply())


    val rdd = sc.textFile(sourceFilePath)
    LogBatchProcess.commonBatch(rdd, service, destFilePath, hbaseSink, kafkaSink, redisSink, useRedis)
  }
}
