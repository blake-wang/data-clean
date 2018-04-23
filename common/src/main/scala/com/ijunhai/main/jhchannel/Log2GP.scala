package com.ijunhai.main.jhchannel

/**
  * Created by admin on 2017/8/10.
  * SparkStreaming从Kafka读取数据，redis记录offset
  */

import java.util
import java.util.{Date, UUID}

import com.ijunhai.common.offset.redisKafkaOffset
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.process.jhchannel.Log2GPProcess
import com.ijunhai.common.logsystem.JunhaiLog
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

object Log2GP {
  var brokerList = ""

  def main(args: Array[String]): Unit = {
    //    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if (args.length < 4) {
      System.err.println("<SERVER:PORT> <TOPIC1,TOPIC2,...> <SECOND> <DEST_TOPIC>")
      System.exit(1)
    }
    val server: String = args(0)
    val topicsStr: String = args(1)
    val second: String = args(2)
    val destTopic = args(3)
    if (server == null || server == "" || topicsStr == null || topicsStr == "" || second == null ||
      second == "" || destTopic == null || destTopic == "") {
      System.err.println("<SERVER:PORT> <TOPIC1,TOPIC2,...> <SECOND> <DEST_TOPIC>")
      System.exit(1)
    }
    val uuid: String = UUID.randomUUID.toString
    val appName = topicsStr + ":Log2GP"
    val sparkConf = new SparkConf().setAppName(appName)
    //      .setMaster("local")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(second.toInt))
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val dbname =if (destTopic.equals(Log2GPProcess.DALAN))
      Log2GPProcess.DALAN_DATABASE
    else
      Log2GPProcess.CHUMENG_DATABASE
    println(dbname)
    val bGreenPlumSimk: Broadcast[GreenPlumSink] = ssc.sparkContext.broadcast(GreenPlumSink.apply(dbname))//

    Save2Kafka.saveMetrics(kafkaSinkBroadcast, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "Log2GP：Total get from kafka", "test", dbname)


    brokerList = server
    Save2Kafka.setBroker(brokerList)
    val group = "Log2GP"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      //"auto.offset.reset" -> "latest",
      //"auto.offset.reset" -> "largest"   //自动将偏移重置为最新偏移（默认）
      "auto.offset.reset" -> "earliest" //自动将偏移重置为最早的偏移
      //"auto.offset.reset" -> "none"      //如果没有为消费者组找到以前的偏移，则向消费者抛出异常
    )
    val redisSinkCluster = ssc.sparkContext.broadcast(RedisSink.apply())

    val topics = topicsStr.split(',').toList
    val fromOffsets = new util.HashMap[TopicPartition, Long]()
    topics.foreach(topic => {
      fromOffsets.putAll(redisKafkaOffset.readOffset(brokerList, topic, group, redisSinkCluster))
    })


    val kafkaStream = if (!fromOffsets.isEmpty)
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    else
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    //    kafkaStream.foreachRDD(rdd => {
    //      println("rdd num:"+rdd.count())
    //    })
    //    println("fromOffsets:"+fromOffsets.isEmpty)
    if (Save2Redis.isRunning(appName, uuid, second.toInt, redisSinkCluster)) {
      println(appName + " is running and the uuid is " + uuid)
      System.exit(1)
    }
    //    kafkaStream.map(_.value()).print()
    //    kafkaStream.map(_.value()).foreachRDD(rdd=>{
    //      rdd.collect().foreach(println)
    //    })

    kafkaStream.map(_.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //        println(rdd.count())
        println("-----------------------------")
        Log2GPProcess.process(rdd, kafkaSinkBroadcast, bGreenPlumSimk, destTopic, redisSinkCluster)
      }
    })
    //    ssc.checkpoint("/user/root/spark/checkpoint")
    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("update offset!")
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //获取offset集合
        topics.foreach(topic => {
          redisKafkaOffset.saveOffset(offsetsList, brokerList, topic, group, redisSinkCluster)
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }
}