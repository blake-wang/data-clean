package com.ijunhai.main.agent

import java.util
import java.util.UUID

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

import scala.collection.JavaConversions._

object Kylin {

  def main(args: Array[String]): Unit = {
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if (args.length < 3) {
      System.err.println("<SOURCE> <SECOND> <DESC>")
      System.exit(1)
    }

    val topicsStr: String = args(0) //YouyunOrder,agentStd,haiwaiStd,hwUserOrder
    val second: String = args(1) //60
    val descTopic = args(2) //kylinLogin,kylinOrder

    val uuid: String = UUID.randomUUID.toString
    val appName = "kylin_clean"
    val sparkConf = new SparkConf().setAppName(appName)
    //      .setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(second.toInt))
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))

    val redisSinkCluster = ssc.sparkContext.broadcast(RedisSink.apply())
    val GPSink = ssc.sparkContext.broadcast(GreenPlumSink.apply(Monitor.database))

    val group: String = "kylin_clean"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Save2Kafka.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "enable.auto.commit" -> "false", //(false: java.lang.Boolean) ,
      "auto.offset.reset" -> "latest"
      //"auto.offset.reset" -> "earliest"
      //"auto.offset.reset" -> "none"      //如果没有为消费者组找到以前的偏移，则向消费者抛出异常
    )

    val topics = topicsStr.split(',').toList
    val fromOffsets = new util.HashMap[TopicPartition, Long]()
    topics.foreach(topic => {
      fromOffsets.putAll(redisKafkaOffset.readOffset(Save2Kafka.brokers, topic, group, redisSinkCluster))
    })


    val kafkaStream = if (!fromOffsets.isEmpty)
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    else
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    if (Save2Redis.isRunning(appName, uuid, second.toInt, redisSinkCluster)) {
      println(appName + " is running and the uuid is " + uuid)
      System.exit(1)
    }

    KafkaLogProcess.kylinProcess(kafkaStream.map(_.value()), kafkaSinkBroadcast, descTopic)

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("update offset!")
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //获取offset集合
        topics.foreach(topic => {
          redisKafkaOffset.saveOffset(offsetsList, Save2Kafka.brokers, topic, group, redisSinkCluster)
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }
}