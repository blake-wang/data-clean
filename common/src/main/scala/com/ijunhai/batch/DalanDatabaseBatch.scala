package com.ijunhai.batch

import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.redis.RedisSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.process.jhchannel.{DB2GPProcess, Log2GPProcess}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/** 批处理dalan youyun后台拖库订单数据关联
  * Created by Admin on 2017-11-20.
  */
object DalanDatabaseBatch {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("DalanDatabaseBatch")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile(args(0))
    val destTopic = args(1)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val bGreenPlumSimk: Broadcast[GreenPlumSink] = sc.broadcast(GreenPlumSink.apply(if (destTopic.equals(Log2GPProcess.DALAN)) Log2GPProcess.DALAN_DATABASE else Log2GPProcess.CHUMENG_DATABASE))
    val redisSinkCluster = sc.broadcast(RedisSink.apply())
    DB2GPProcess.processBatch(rdd, kafkaSinkBroadcast, bGreenPlumSimk, destTopic, redisSinkCluster)
  }
}
