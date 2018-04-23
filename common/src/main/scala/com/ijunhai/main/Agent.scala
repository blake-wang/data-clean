package com.ijunhai.main

import java.util
import java.util.UUID

import com.ijunhai.common.logsystem.Monitor
import com.ijunhai.common.offset.{Offset, redisKafkaOffset}
import com.ijunhai.common.{CleanConstants, HDFSUtil}
import com.ijunhai.main.agent.AgentActive.brokerList
import com.ijunhai.process.CommonProcess
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.hbase.HbaseSink
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

object Agent {
  val second = "60"

  def main(args: Array[String]): Unit = {
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val source = args(0)
    val topicsStr = args(1)
//        val source = "haiwaiLoginSrc"
//        val topicsStr = "agentLoginSrc,agentOldH5LoginSrc"


    val uuid: String = UUID.randomUUID.toString
    val appName = "agent:" + source
    val sparkConf = new SparkConf().setAppName(appName)
//          .setMaster("local")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(second.toInt))
    val kafkaSink = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSink = ssc.sparkContext.broadcast(RedisSink.apply())
    val gpSink = ssc.sparkContext.broadcast(GreenPlumSink.apply(Monitor.database))
    val hbaseSink = ssc.sparkContext.broadcast(HbaseSink.apply())

    Save2Kafka.setBroker(Save2Kafka.brokers)
    val group: String = "agent"
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
      fromOffsets.putAll(Offset.readOffset(topic, group, redisSink))
    })
    val dataCleanConfig: Document = HDFSUtil.readConfigFromHdfs(CleanConstants.configPath)
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

    kafkaStream.map(_.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        if (source.contains("Login")){
          CommonProcess.loginProcess(rdd, source, redisSink, kafkaSink, gpSink, hbaseSink)
        }else{
          CommonProcess.orderProcess(rdd, source, redisSink, kafkaSink, gpSink, hbaseSink)
        }

      }
    })

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        topics.foreach(topic => {
          Offset.saveOffset(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, topic, group, redisSink)
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}