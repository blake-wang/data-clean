package com.ijunhai.batch.agent

import com.ijunhai.batch.process.CommonProcessBatch
import com.ijunhai.common.logsystem.Monitor
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AgentBatch {
  def main(args: Array[String]) {
    val source = args(0)
    val sourceFilePath = args(1)
    val userRedis = if (args(2)=="true") true else false

    val sparkConf = new SparkConf().setAppName("LogBatch")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[redis.clients.jedis.Jedis]))
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Connection]))


    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    val sc = new SparkContext(sparkConf)

    val hbaseSink = sc.broadcast(HbaseSink.apply())
    val kafkaSink = sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSink = sc.broadcast(RedisSink.apply())
    val gpSink = sc.broadcast(GreenPlumSink.apply(Monitor.database))
    val rdd = sc.textFile(sourceFilePath)
    if (!rdd.isEmpty()) {
      if (source.contains("Login")){
        CommonProcessBatch.loginProcess(rdd, source, redisSink, kafkaSink, gpSink, hbaseSink,userRedis)
      }else{
        CommonProcessBatch.orderProcess(rdd, source, redisSink, kafkaSink, gpSink, hbaseSink,userRedis)
      }
    }
  }
}
