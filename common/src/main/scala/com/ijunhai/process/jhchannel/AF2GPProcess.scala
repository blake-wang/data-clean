package com.ijunhai.process.jhchannel

import java.util
import java.util.Date

import com.ijunhai.common.logsystem.{HaiwaiOrder, HaiwaiUser, JunhaiLog}
import com.ijunhai.common.offset.redisRocketMQOffset
import com.ijunhai.process.CommonProcess
import com.ijunhai.process.agent.AgentProcess
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.kafka.KafkaSink
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{OffsetRange, TopicQueueId}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConversions._

object AF2GPProcess {

  val HAIWAI_DATABASE = "youyun_hw"
  val HAIWAI_TABLE_USER_NAME = "hw_user"
  val TABLE_ORDER_NAME = "hw_order"


  def process(rdd: RDD[MessageExt]
              , kafkaSink: Broadcast[KafkaSink]
              , bGreenPlumSink: Broadcast[GreenPlumSink]
              , topic: String
              , offset: java.util.Map[TopicQueueId, Array[OffsetRange]]
              , nameServer: String
              , topics: java.util.List[String]
              , group: String
              , hdfsCachePath: String
              , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    val ORDER = "order"
    val USER = "user"
    println("------------" + System.currentTimeMillis() + "------------")
    if (!rdd.isEmpty()) {
      val rows = "rows"
      val table = "table"
      val type_ = "type"
      val data = "data"
      //val hdfsCachePath = ""
      val (rmb, usd) = AgentProcess.getRate(hdfsCachePath)

      val bRmb = rdd.sparkContext.broadcast(rmb)
      val bUsd = rdd.sparkContext.broadcast(usd)
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
        //Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), "com.ijunhai.main.jhchannel.AF2GP", JunhaiLog.INFO, s"Total get from rocketMQ $nameServer and topic(s) $topicsStr", "Num", rdd.count() + "")
        //Save2Kafka.monitor(kafkaSink, dataSystem, ip, Save2Kafka.receivedFromMQ, length, Save2Kafka.logFile, filePath)

      val msgs = CommonProcess.extraData(rdd, kafkaSink, nameServer, topicsStr)
      //  msgs.foreach(x => println(x))
      val document = msgs.map { case (msg, system) => {
        //msgs.distinct()
        try {
          val doc = Document.parse(msg)
          (doc, true)
        } catch {
          case e: Exception =>
            System.err.println(new Date() + " Error: json parse error!")
            (new Document(AgentProcess.LOG, msg), false)
        }
      }
      }
      //Save2Kafka.saveDStreamAsync(document.filter(!_._2).map(_._1.toJson()), kafkaSink, topic + AgentProcess.ERROR_FLAG, nameServer + ":com.ijunhai.main.jhchannel.AF2GPProcess")

      val filterDocuments = document.filter(_._2).flatMap(line => {
        line._1.get(rows).asInstanceOf[util.ArrayList[Document]].map(doc => {
          val tableName = JunhaiLog.getString(doc, table)
          val typeAction = JunhaiLog.getString(doc, type_)
          (doc.get(data).asInstanceOf[Document], tableName, typeAction)
        })
      })

      val orderData = filterDocuments.filter { case (document, tableName, type_) => {
        //表两条日志
        // 1、pay_status=0 and status=1 下单
        // 2、pay_status=1 and status=2 支付收到钱。
        tableName == ORDER && ((JunhaiLog.getInt(document, "pay_status") == 0 && JunhaiLog.getInt(document, "status") == 1)
          || (JunhaiLog.getInt(document, "pay_status") == 1 && JunhaiLog.getInt(document, "status") == 2))
      }
      }

      val userData = filterDocuments.filter { case (document, tableName,type_ ) => {
        tableName == USER && type_ == "INSERT"
      }
      }
      //关联汇率数据 放入每个document对象里
      val RorderData = orderData.map(line => {
        val document = line._1
        val money = JunhaiLog.getDouble(document, HaiwaiOrder.money)
        val money_type = JunhaiLog.getString(document, HaiwaiOrder.money_type)
        val rmb = bRmb.value.getOrDefault(money_type, 1.0) * money
        val usd = bUsd.value.getOrDefault(money_type, 1.0) * money
        document.append(HaiwaiOrder.rmb, rmb)
        document.append(HaiwaiOrder.usd, usd)
        document
      })
      //Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), AF2GPProcess.getClass.getName, JunhaiLog.INFO, "AF2GP: from dlUserOrder ", "Num", "orderData:" + orderData.count() + " userData:" + userData.count())

      //存GP
      val orderDocument = orderTransform(RorderData, bGreenPlumSink, TABLE_ORDER_NAME).count()
      val userDocument = userTransform(userData.map(_._1), bGreenPlumSink, topic, redisSinkCluster).count()
      //  Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "DB2GP: after save to GP ", "Num", "orderDocument:" + orderDocument.count() + " userDocument:" + userDocument.count())

      //存kafka
      //Save2Kafka.saveDStreamAsync(orderDocument.map(_.toJson()), kafkaSink, topic + "Order", "com.ijunhai.process.jhchannel.AF2GP")
      //Save2Kafka.saveDStreamAsync(userDocument.map(_.toJson()), kafkaSink, topic + "User", "com.ijunhai.process.jhchannel.AF2GP")

      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }

 /* /**
    * 订单关联处理
    *
    * @param rdd   rocketMQ未经处理的数据
    * @param topic 写到kafka的topic
    */
  def processBatch(rdd: RDD[String]
                   , kafkaSink: Broadcast[KafkaSink]
                   , bGreenPlumSink: Broadcast[GreenPlumSink]
                   , topic: String
                   , redisSinkCluster: Broadcast[RedisSinkCluster]): Unit = {
    val ORDER = "order"
    val USER = "user"
    println("------------" + System.currentTimeMillis() + "------------")
    if (!rdd.isEmpty()) {
      val rows = "rows"
      val table = "table"
      val type_ = "type"
      val data = "data"
      val document = rdd.map { case msg => {
        try {
          val doc = Document.parse(msg)
          (doc, true)
        } catch {
          case e: Exception =>
            System.err.println(new Date() + " Error: json parse error!")
            (new Document(AgentProcess.LOG, msg), false)
        }
      }
      }

      Save2Kafka.saveDStreamAsync(document.filter(!_._2).map(_._1.toJson()), kafkaSink, topic + AgentProcess.ERROR_FLAG, ":com.ijunhai.kafka.mq2kafka.DalanDatabaseProcessBatch")
      val filterDocuments = document.filter(_._2).flatMap(line => {
        line._1.get(rows).asInstanceOf[util.ArrayList[Document]].map(doc => {
          val tableName = JunhaiLog.getString(doc, table)
          val typeAction = JunhaiLog.getString(doc, type_)
          (doc.get(data).asInstanceOf[Document], tableName, typeAction)
        })
      })

      val orderData = filterDocuments.filter { case (document, tableName, type_) => {
        //表两条日志
        // 1、pay_status=0 and status=1 下单
        // 2、pay_status=1 and status=2 支付收到钱。
        tableName == ORDER && ((JunhaiLog.getInt(document, "pay_status") == 0 && JunhaiLog.getInt(document, "status") == 1)
          || (JunhaiLog.getInt(document, "pay_status") == 1 && JunhaiLog.getInt(document, "status") == 2))
      }
      }

      val userData = filterDocuments.filter { case (document, tableName, type_) => {
        tableName == USER && type_ == "INSERT"
      }
      }
      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "DB2GP: from dlUserOrder ", "Num", "orderData:" + orderData.count() + " userData:" + userData.count())

      val orderDocument = orderTransform(orderData.map(_._1), bGreenPlumSink, TABLE_ORDER_NAME)
      //val userDocument = userTransform(userData.map(_._1), bGreenPlumSink, HAIWAI_DATABASE, redisSinkCluster)
      //Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "DB2GP: after save to GP ", "Num", "orderDocument:" + orderDocument.count() + " userDocument:" + userDocument.count())


      Save2Kafka.saveDStreamAsync(orderDocument.map(_.toJson()), kafkaSink, topic + "Order", "com.ijunhai.kafka.mq2kafka.DalanDatabaseProcess")
      // Save2Kafka.saveDStreamAsync(userDocument.map(_.toJson()), kafkaSink, topic + "User", "com.ijunhai.kafka.mq2kafka.DalanDatabaseProcess")
    }
  }*/

  def orderTransform(rDD: RDD[Document]
                     , bGreenPlumSink: Broadcast[GreenPlumSink]
                     , table: String): RDD[Document] = {
    val documents = rDD.map(document => {

      HaiwaiOrder.unload(HaiwaiOrder.load(document))
    })
    Save2GP.saveDStream(documents, bGreenPlumSink, table)
  }

  def userTransform(rDD: RDD[Document], bGreenPlumSink: Broadcast[GreenPlumSink], topic: String, redisSinkCluster: Broadcast[RedisSink]): RDD[Document] = {
    val table =HAIWAI_TABLE_USER_NAME
    val documents = rDD.map(document => {
      HaiwaiUser.unload(HaiwaiUser.load(document))
    })
    val result = Save2Redis.isNewDevice(documents, topic, HaiwaiUser.game, HaiwaiUser.device_id, HaiwaiUser.register_time, redisSinkCluster, JunhaiLog.eventRegister)
      .map { case (document, isNewDevice) => {
        document.append(HaiwaiUser.new_device, isNewDevice)
      }
      }
    //    documents.foreach(println)
    Save2GP.saveDStream(result, bGreenPlumSink, table)
  }

  /*/**
    * 订单关联处理
    *
    * @param rdd   rocketMQ未经处理的数据
    * @param topic 写到kafka的topic
    */
  def processTemp(rdd: RDD[String]
                  , kafkaSink: Broadcast[KafkaSink]
                  , topic: String): Unit = {
    val ORDER = "order"
    val USER = "user"
    println("------------" + System.currentTimeMillis() + "------------")
    if (!rdd.isEmpty()) {
      val rows = "rows"
      val table = "table"
      val type_ = "type"
      val data = "data"

      val document = rdd.map { case msg => {
        try {
          val doc = Document.parse(msg)
          (doc, true)
        } catch {
          case e: Exception =>
            System.err.println(new Date() + " Error: json parse error!")
            (new Document(AgentProcess.LOG, msg), false)
        }
      }
      }
      println("error:" + document.filter(!_._2).count())
      //      Save2Kafka.saveDStreamAsync(document.filter(!_._2).map(_._1.toJson()),kafkaSink,topic+DataProcess.ERROR_FLAG,":com.ijunhai.kafka.mq2kafka.DalanDatabaseProcessBatch")
      val filterDocuments = document.filter(_._2).flatMap(line => {
        line._1.get(rows).asInstanceOf[util.ArrayList[Document]].map(doc => {
          val tableName = JunhaiLog.getString(doc, table)
          val typeAction = JunhaiLog.getString(doc, type_)
          (doc.get(data).asInstanceOf[Document], tableName, typeAction)
        })
      })

      val orderData = filterDocuments.filter { case (document, tableName, type_) => {
        //表两条日志
        // 1、pay_status=0 and status=1 下单
        // 2、pay_status=1 and status=2 支付收到钱。
        tableName == ORDER && JunhaiLog.getInt(document, "pay_status") == 0 && JunhaiLog.getInt(document, "status") == 1 &&
          JunhaiLog.getString(document, "create_time").contains("2017-11-26")
      }
      }
      println("orderData:" + orderData.count())
    }
  }*/
}
