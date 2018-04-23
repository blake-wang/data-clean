package com.ijunhai.main.agent

import java.util.{Date, UUID}

import com.ijunhai.common.HDFSUtil
import com.ijunhai.common.logsystem.{JunhaiLog, Monitor}
import com.ijunhai.process.agent.KafkaLogProcess
import com.ijunhai.process.agent.KafkaLogProcess.{LOG, error}
import com.ijunhai.storage.greenplum
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document

object Kafka2SparkTest {

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
//10.13.4.84:9092,10.13.112.92:9092,10.13.18.222:9092 haiwai 60 haiwai hdfs://Ucluster/data/config/old-agent-cleanConfig.xml hdfs://Ucluster/data/ip_database/17monipdb.dat
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
          .setMaster("local")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(second.toInt))
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val brokerList = server
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
      "auto.offset.reset" -> "latest"
      //      "auto.offset.reset" -> "earliest"
      //"auto.offset.reset" -> "none"      //如果没有为消费者组找到以前的偏移，则向消费者抛出异常
    )

    val topics = topicsStr.split(',').toList
//    val fromOffsets = new util.HashMap[TopicPartition, Long]()
//    topics.foreach(topic => {
//      fromOffsets.putAll(redisKafkaOffset.readOffset(brokerList, topic, group, redisSinkCluster))
//    })


    val dataCleanConfig: Document = HDFSUtil.readConfigFromHdfs(configPath)
    if (dataCleanConfig.isEmpty) {
      throw new IllegalArgumentException
    }


    //    val kafkaStream = if (!fromOffsets.isEmpty)
    //      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    //    else
    val kafkaStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    //    if (Save2Redis.isRunning(appName, uuid, second.toInt, redisSinkCluster)) {
    //      println(appName + " is running and the uuid is " + uuid)
    //      System.exit(1)
    //    }


    kafkaStream.map(_.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val resultRDD = rdd.map(log => {
         JunhaiLog.log2bson(log)._1
        })
//        greenplum.Save2GP.saveDStream(resultRDD,GPSink,"monitor")
//        Save2Kafka.saveDStreamAsync(resultRDD.map(_.toJson()),kafkaSinkBroadcast,"")
      }})



//    KafkaLogProcess.process(kafkaStream.map(_.value()), kafkaSinkBroadcast, service, dataCleanConfig, ipDatabasePath, redisSinkCluster, server, GPSink)

    //    kafkaStream.foreachRDD(rdd => {
    //      if (!rdd.isEmpty()) {
    //        println("update offset!")
    //        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //        topics.foreach(topic => {
    //          redisKafkaOffset.saveOffset(offsetsList, brokerList, topic, group, redisSinkCluster)
    //        })
    //      }
    //    })
    ssc.start()
    ssc.awaitTermination()

  }
}