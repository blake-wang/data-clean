package com.ijunhai.batch

import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.redis.RedisSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.process.jhchannel.Log2GPProcess
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**批处理的大蓝youyun后台日志数据处理程序
  * Created by Admin on 2017-11-20.
  */
object Log2GPBatch {
  def main(args: Array[String]) {
    val sparkConf=new SparkConf()
            .setMaster("local")
      .setAppName("Log2GPBatch")
    val sc=new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSinkCluster = sc.broadcast(RedisSink.apply())
    val rdd=sc.textFile("file:///C:\\Users\\Admin\\Documents\\NetSarang\\Xshell\\Sessions\\dalan.txt")
    val destTopic=""
    val bGreenPlumSimk:Broadcast[GreenPlumSink]=sc.broadcast(GreenPlumSink.apply(if (destTopic.equals(Log2GPProcess.DALAN)) Log2GPProcess.DALAN_DATABASE else Log2GPProcess.CHUMENG_DATABASE))
    Log2GPProcess.process(rdd,kafkaSinkBroadcast,bGreenPlumSimk,destTopic,redisSinkCluster)

  }
}
