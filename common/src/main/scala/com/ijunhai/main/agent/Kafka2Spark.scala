package com.ijunhai.main.agent

import java.util
import java.util.UUID

import com.ijunhai.common.HDFSUtil
import com.ijunhai.common.logsystem.Monitor
import com.ijunhai.common.offset.redisKafkaOffset
import com.ijunhai.process.agent.KafkaLogProcess
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document

import scala.collection.JavaConversions._

object Kafka2Spark {
  var brokerList = ""

  def main(args: Array[String]): Unit = {
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if (args.length < 6) {
      System.err.println("<SERVER:PORT> <TOPIC1,TOPIC2,...> <SECOND> <SERVICE> <CONFIG_PATH> <IP_PATH>")
      System.exit(1)
    }
    val server: String = args(0)
    val topicsStr: String = args(1)
    val second: String = args(2)
    val service = args(3)
    val configPath = args(4)
    val ipDatabasePath = args(5)

    //    val server: String ="192.168.1.112:9092,192.168.1.110:9092,192.168.1.111:9092"
    //    val topicsStr: String = "agentLogin"
    //    val second: String = "60"
    //    val service = "agent"
    //    val configPath = "hdfs://slave-02:8022/data/config/old-agent-cleanConfig.xml"
    //    val ipDatabasePath = "hdfs://slave-02:8022/data/ip_database/17monipdb.dat"


    if (server == null || server == "" || topicsStr == null || topicsStr == "" || second == null ||
      second == "" || service == null || service == "" || configPath == null || configPath == "") {
      System.err.println("<SERVER:PORT> <TOPIC1,TOPIC2,...> <SECOND> <SERVICE> <CONFIG_PATH> <IP_PATH>")
      System.exit(1)
    }

    val uuid: String = UUID.randomUUID.toString
    val appName = service + (if (service.equals("agent")) ":Kafka2SparkOld" else ":Kafka2Spark")
    val sparkConf = new SparkConf().setAppName(appName)
    //      .setMaster("local")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(second.toInt))
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    brokerList = server
    val redisSinkCluster = ssc.sparkContext.broadcast(RedisSink.apply())
    val GPSink = ssc.sparkContext.broadcast(GreenPlumSink.apply(Monitor.database))


    Save2Kafka.setBroker(brokerList)
    val group: String = if (service.equals("agent")) "Kafka2SparkOld" else "Kafka2Spark"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerList,
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
      fromOffsets.putAll(redisKafkaOffset.readOffset(brokerList, topic, group, redisSinkCluster))
    })
    val dataCleanConfig: Document = HDFSUtil.readConfigFromHdfs(configPath)
    if (dataCleanConfig.isEmpty) {
      throw new IllegalArgumentException
    }
    println(dataCleanConfig)

    val kafkaStream = if (!fromOffsets.isEmpty)
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    else
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    if (Save2Redis.isRunning(appName, uuid, second.toInt, redisSinkCluster)) {
      println(appName + " is running and the uuid is " + uuid)
      System.exit(1)
    }

    KafkaLogProcess.process(kafkaStream.map(_.value()), kafkaSinkBroadcast, service, dataCleanConfig, ipDatabasePath, redisSinkCluster,server,GPSink)

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("update offset!")
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        topics.foreach(topic => {
          redisKafkaOffset.saveOffset(offsetsList, brokerList, topic, group, redisSinkCluster)
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }
}