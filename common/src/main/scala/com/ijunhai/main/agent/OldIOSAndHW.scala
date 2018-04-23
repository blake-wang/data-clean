package com.ijunhai.main.agent

/**
  * Created by admin on 2017/8/10.
  * SparkStreaming从Kafka读取数据，redis记录offset
  */

import java.util

import com.ijunhai.common.offset.redisKafkaOffset
import com.ijunhai.process.agent.KafkaLogProcess
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
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

object OldIOSAndHW {
  var brokerList=""
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if(args.length<5){
      System.err.println("<SERVER:PORT> <READ_TOPIC> <SECOND> <HDFS_CACHE_PATH> <WRITE_TOPIC>")
      System.exit(1)
    }
    val server: String = args(0)
    val topicsStr: String = args(1)
    val system=topicsStr
    val second: String = args(2)
    val hdfsCachePath=args(3)
    val finalDesTopic=args(4)
    if (server == null || server == "" || topicsStr == null || topicsStr == "" || second == null ||
      second == "" || hdfsCachePath==null || hdfsCachePath=="" || finalDesTopic==null || finalDesTopic=="") {
      System.err.println("<SERVER:PORT> <READ_TOPIC> <SECOND> <HDFS_CACHE_PATH> <WRITE_TOPIC>")
      System.exit(1)
    }
    val sparkConf=new SparkConf().setAppName("JunhaiChannelDatabase")
    //      .setMaster("local")
    val ssc: StreamingContext = new StreamingContext(sparkConf , Seconds(second.toInt))
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    brokerList = server
    Save2Kafka.setBroker(brokerList)
    val group = "JunhaiChannelDatabase"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group
      //"enable.auto.commit" -> (false: java.lang.Boolean) ,
      //"auto.offset.reset" -> "latest",
      //"auto.offset.reset" -> "largest"   //自动将偏移重置为最新偏移（默认）
      //"auto.offset.reset" -> "earliest"  //自动将偏移重置为最早的偏移
      //"auto.offset.reset" -> "none"      //如果没有为消费者组找到以前的偏移，则向消费者抛出异常
    )
    val redisSinkCluster = ssc.sparkContext.broadcast(RedisSink.apply())

    val topics = topicsStr.split(',').toList
    val fromOffsets: util.HashMap[TopicPartition, Long] =new util.HashMap[TopicPartition, Long]()
    topics.foreach(topic=>{
      fromOffsets.putAll(redisKafkaOffset.readOffset(brokerList, topic, group,redisSinkCluster))
    })

    val kafkaStream = if(!fromOffsets.isEmpty)
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    else
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    println("fromOffsets:"+fromOffsets.isEmpty)

    //    ssc.checkpoint("/user/root/spark/checkpoint")
    ssc.start()
    ssc.awaitTermination()
    KafkaLogProcess.oldIOSAndHW(kafkaStream.map(_.value()),kafkaSinkBroadcast, "channel_id", finalDesTopic, hdfsCachePath,system,redisSinkCluster)
    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("update offset!")
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //获取offset集合
        topics.foreach(topic=>{
          redisKafkaOffset.saveOffset(offsetsList, brokerList, topic, group,redisSinkCluster)
        })
      }
    })
  }
}