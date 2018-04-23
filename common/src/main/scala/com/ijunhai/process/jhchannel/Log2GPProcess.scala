package com.ijunhai.process.jhchannel

import java.util.Date

import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.process.agent.KafkaLogProcess
import com.ijunhai.common.logsystem.{DalanActive, DalanLogin, JunhaiLog, Monitor}
import com.ijunhai.common.offset.redisRocketMQOffset
import com.ijunhai.process.CommonProcess
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{OffsetRange, TopicQueueId}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConversions._

/**
  * Created by Admin on 2017-11-09.
  */
object Log2GPProcess {
  val DALAN = "dalan"
  val CHUMENG = "chumeng"

  val DALAN_DATABASE = "youyun_log"
  val DALAN_TABLE_LOGIN_NAME = "dl_login_log"
  val DALAN_TABLE_ACTIVE_NAME = "dl_active_log"
  val CHUMENG_DATABASE = "chumeng_log"
  val CHUMENG_TABLE_LOGIN_NAME = "cm_login_log"
  val CHUMENG_TABLE_ACTIVE_NAME = "cm_active_log"

  def tmpLogProcess(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink], greenPlumSink: Broadcast[GreenPlumSink], offset: java.util.Map[TopicQueueId
    , Array[OffsetRange]], nameServer: String, topics: java.util.List[String], group: String
                 , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    if (!rdd.isEmpty()) {
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      val msgs = CommonProcess.extra(rdd, greenPlumSink, nameServer, topicsStr)
      Log2GPProcess.process(msgs.map(_._1), kafkaSink, greenPlumSink, "dalan", redisSinkCluster)
      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }


  def process(rDD: RDD[String], kafkaSink: Broadcast[KafkaSink]
              , bGreenPlumSink: Broadcast[GreenPlumSink]
              , topic: String
              , redisSinkCluster: Broadcast[RedisSink]): Unit = {
//    Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "Log2GP：Total get from kafka", "Num", rDD.count() + "")
    val allLogs = rDD.map(log => {
      val document = try {
        JunhaiLog.log2bson(log)
      } catch {
        case e: Exception =>
          (new Document(KafkaLogProcess.LOG, log), false)
      }
      val event = if (document._2) {
        JunhaiLog.getString(document._1, JunhaiLog.event)
      } else {
        ""
      }
      (document._1, event, document._2)
    })
    val rightAllLogs = allLogs.filter(_._3)
    val errorAllLogs = allLogs.filter(!_._3)
    //    Save2Kafka.saveMetrics(kafkaSink,JunhaiLog.MONITOR_TOPIC,new Date(),Log2GPProcess.getClass.getName,JunhaiLog.INFO,"Log2GP all error log","Num",errorAllLogs.count()+"")
    //    Save2Kafka.saveMetrics(kafkaSink,JunhaiLog.MONITOR_TOPIC,new Date(),Log2GPProcess.getClass.getName,JunhaiLog.INFO,"Log2GP login error log","Num"
    //      ,errorAllLogs.filter(_._2==JunhaiLog.eventLogin).count()+"")
    //    Save2Kafka.saveDStreamAsync(errorAllLogs.map(_._1.toJson()),kafkaSink,topic+"Login"+"Error")

    val loginLogs = rightAllLogs.filter(_._2 == JunhaiLog.eventLogin).map(_._1)
    val activeLogs = rightAllLogs.filter(_._2 == JunhaiLog.eventActive).map(_._1)

    Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "Log2GP：Log2GP logs test" + (if (topic.equals(DALAN)) Log2GPProcess.DALAN_TABLE_LOGIN_NAME else Log2GPProcess.CHUMENG_TABLE_LOGIN_NAME), "Num", "active:" + activeLogs.count() + "login:" + loginLogs.count())

    //GP
    val loginDocument = loginTransfor(loginLogs, bGreenPlumSink, if (topic.equals(DALAN)) Log2GPProcess.DALAN_TABLE_LOGIN_NAME else Log2GPProcess.CHUMENG_TABLE_LOGIN_NAME)
    val activeDocument = activeTransfor(activeLogs, bGreenPlumSink, topic, redisSinkCluster, kafkaSink)

    //kafka
    Save2Kafka.saveDStreamAsync(activeDocument.map(_.toJson()), kafkaSink, topic + "Active", "com.ijunhai.process.Log2GPProcess")
    Save2Kafka.saveDStreamAsync(loginDocument.map(_.toJson()), kafkaSink, topic + "Login", "com.ijunhai.process.Log2GPProcess")


  }

  def loginTransfor(rDD: RDD[Document], bGreenPlumSink: Broadcast[GreenPlumSink], table: String): RDD[Document] = {
    val documents = rDD.map(doc => {
      val dalanLogin = DalanLogin.load(doc)
      DalanLogin.unload(dalanLogin)
    })
    Save2GP.saveDStream(documents, bGreenPlumSink, table)
  }

  def activeTransfor(rDD: RDD[Document], bGreenPlumSink: Broadcast[GreenPlumSink]
                     , topic: String, redisSinkCluster: Broadcast[RedisSink]
                     , kafkaSink: Broadcast[KafkaSink]): RDD[Document] = {
    val table = if (topic.equals(DALAN)) Log2GPProcess.DALAN_TABLE_ACTIVE_NAME else Log2GPProcess.CHUMENG_TABLE_ACTIVE_NAME
    val documents = rDD.map(doc => {
      val dalanActive = DalanActive.load(doc)
      DalanActive.unload(dalanActive)
    })

    //去redis数据库中关联用户注册信息
    val result: RDD[Document] = Save2Redis.isNewDevice(documents, topic, DalanActive.union_app_id, DalanActive.device_id, DalanActive.create_time, redisSinkCluster, JunhaiLog.eventActive)
      //      .filter(_._2 == 1)   唯一键是union_app_id + device_id 通过唯一键过滤，不需要再过滤根据redis数据过滤
      .map(_._1)
    //    Save2GreenPlum.saveDStreamTest(result, bGreenPlumSink, table,kafkaSink)

    Save2GP.saveDStream(result, bGreenPlumSink, table)
  }
}
