package com.ijunhai.main


import java.util
import java.util.UUID

import com.ijunhai.common.HDFSUtil
import com.ijunhai.common.logsystem.Monitor
import com.ijunhai.common.offset.redisKafkaOffset
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document

import scala.collection.JavaConversions._

object Channel {
  val configPath = "hdfs://Ucluster/data/config/old-agent-cleanConfig.xml"
  val ipDatabasePath = "hdfs://Ucluster/data/ip_database/17monipdb.dat"
  val second = "60"
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val source = args(0)
    val topicsStr =args(1)

    val uuid: String = UUID.randomUUID.toString
    val appName = "Channel" + source
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(second.toInt))
    val kafkaSink = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSink = ssc.sparkContext.broadcast(RedisSink.apply())
    val gpSink = ssc.sparkContext.broadcast(GreenPlumSink.apply(Monitor.database))

    Save2Kafka.setBroker(Save2Kafka.brokers)
    val group: String = "agentActive"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Save2Kafka.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "enable.auto.commit" -> "false", //(false: java.lang.Boolean) ,
      //"auto.offset.reset" -> "latest",
      "auto.offset.reset" -> "earliest"
    )




    val topics = topicsStr.split(',').toList
    val fromOffsets = new util.HashMap[TopicPartition, Long]()
    topics.foreach(topic => {
      fromOffsets.putAll(redisKafkaOffset.readOffset(Save2Kafka.brokers, topic, group, redisSink))
    })
    val dataCleanConfig: Document = HDFSUtil.readConfigFromHdfs(configPath)
    if (dataCleanConfig.isEmpty) {
      throw new IllegalArgumentException
    }
    val kafkaStream = if (!fromOffsets.isEmpty)
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    else
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    if (Save2Redis.isRunning(appName, uuid, second.toInt, redisSink)) {
      println(appName + " is running and the uuid is " + uuid)
      System.exit(1)
    }

    source match {
      case "chumeng" =>
      case "cmUserOrder" =>
      case "junhai" =>
      case "jhUserOrder" =>
      case "dalan" =>
      case "dlUserOrder"=>
    }




    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        topics.foreach(topic => {
          redisKafkaOffset.saveOffset(offsetsList, Save2Kafka.brokers, topic, group, redisSink)
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }
}