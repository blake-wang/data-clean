package com.ijunhai.process.jhchannel

import java.util
import java.util.Date

import com.ijunhai.common.offset.redisRocketMQOffset
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.process.agent.AgentProcess
import com.ijunhai.common.logsystem.{DalanOrder, DalanUser, JunhaiLog}
import com.ijunhai.process.CommonProcess
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{OffsetRange, TopicQueueId}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConversions._

object DB2GPProcess {
  val DALAN_DATABASE = "youyun_log"
  val DALAN_TABLE_USER_NAME = "dl_user"
  val TABLE_ORDER_NAME = "order_log"
  val CHUMENG_DATABASE = "chumeng_log"
  val CHUMENG_TABLE_USER_NAME = "cm_user"

  def process(rdd: RDD[MessageExt]
              , kafkaSink: Broadcast[KafkaSink]
              , bGreenPlumSink: Broadcast[GreenPlumSink]
              , topic: String
              , offset: java.util.Map[TopicQueueId, Array[OffsetRange]]
              , nameServer: String
              , topics: java.util.List[String]
              , group: String
              , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    val ORDER = "order"
    val USER = "user"
    println("------------" + System.currentTimeMillis() + "------------")
    if (!rdd.isEmpty()) {
      val rows = "rows"
      val table = "table"
      val type_ = "type"
      val data = "data"

      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), "com.ijunhai.kafka.mq2kafka.DB2GP", JunhaiLog.INFO, s"Total get from rocketMQ $nameServer and topic(s) $topicsStr", "Num", rdd.count() + "")
      //      Save2Kafka.monitor(kafkaSink, dataSystem, ip, Save2Kafka.receivedFromMQ, length, Save2Kafka.logFile, filePath)

      val msgs = CommonProcess.extraData(rdd, kafkaSink, nameServer, topicsStr)
      val document = msgs.map { case (msg, system) => { //msgs.distinct()
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

      Save2Kafka.saveDStreamAsync(document.filter(!_._2).map(_._1.toJson()), kafkaSink, topic + AgentProcess.ERROR_FLAG, nameServer + ":com.ijunhai.kafka.mq2kafka.DalanDatabaseProcess")

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
        //1 4
        tableName == ORDER && ((JunhaiLog.getInt(document, "pay_status") == 0 && JunhaiLog.getInt(document, "status") == 1)
          || (JunhaiLog.getInt(document, "pay_status") == 1 && JunhaiLog.getInt(document, "status") == 2))
      }
      }

      val userData = filterDocuments.filter { case (document, tableName, type_) => {
        tableName == USER && type_ == "INSERT"
      }
      }

      //      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "DB2GP: from dlUserOrder ", "Num", "orderData:" + orderData.count() + " userData:" + userData.count())

      //存GP
      val orderDocument = orderTransform(orderData.map(_._1), bGreenPlumSink, TABLE_ORDER_NAME)
      val userDocument = userTransform(userData.map(_._1), bGreenPlumSink, topic, redisSinkCluster)
      //      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "DB2GP: after save to GP ", "Num", "orderDocument:" + orderDocument.count() + " userDocument:" + userDocument.count())

      //存kafka
      Save2Kafka.saveDStreamAsync(orderDocument.map(_.toJson()), kafkaSink, topic + "Order", "com.ijunhai.kafka.mq2kafka.DB2GP")
      Save2Kafka.saveDStreamAsync(userDocument.map(_.toJson()), kafkaSink, topic + "User", "com.ijunhai.kafka.mq2kafka.DB2GP")

      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }

  /**
    * 订单关联处理
    *
    * @param rdd   rocketMQ未经处理的数据
    * @param topic 写到kafka的topic
    */
  def processBatch(rdd: RDD[String]
                   , kafkaSink: Broadcast[KafkaSink]
                   , bGreenPlumSink: Broadcast[GreenPlumSink]
                   , topic: String
                   , redisSinkCluster: Broadcast[RedisSink]): Unit = {
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
      val userDocument = userTransform(userData.map(_._1), bGreenPlumSink, if (topic.equals("dalan")) DALAN_TABLE_USER_NAME else CHUMENG_TABLE_USER_NAME, redisSinkCluster)
      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "DB2GP: after save to GP ", "Num", "orderDocument:" + orderDocument.count() + " userDocument:" + userDocument.count())


      Save2Kafka.saveDStreamAsync(orderDocument.map(_.toJson()), kafkaSink, topic + "Order", "com.ijunhai.kafka.mq2kafka.DalanDatabaseProcess")
      Save2Kafka.saveDStreamAsync(userDocument.map(_.toJson()), kafkaSink, topic + "User", "com.ijunhai.kafka.mq2kafka.DalanDatabaseProcess")
    }
  }

  def orderTransform(rDD: RDD[Document]
                     , bGreenPlumSink: Broadcast[GreenPlumSink]
                     , table: String): RDD[Document] = {
    val documents = rDD.map(document => {
      DalanOrder.unload(DalanOrder.load(document))
    })
    Save2GP.saveDStream(documents, bGreenPlumSink, table)
  }

  def userTransform(rDD: RDD[Document], bGreenPlumSink: Broadcast[GreenPlumSink], topic: String, redisSinkCluster: Broadcast[RedisSink]): RDD[Document] = {
    val table = if (topic.equals("dalan")) DALAN_TABLE_USER_NAME else CHUMENG_TABLE_USER_NAME
    val documents = rDD.map(document => {
      DalanUser.unload(DalanUser.load(document))
    })

    // id+user_id+user_name+tel_num+tel_status+email+reg_type+channel+game+
    // device_id+register_ip+register_time+status+platform+pf+new_device
    val result = Save2Redis.isNewDevice(documents, topic, DalanUser.game, DalanUser.device_id, DalanUser.register_time, redisSinkCluster, JunhaiLog.eventRegister)
      .map { case (document, isNewDevice) => {
        document.append(DalanUser.new_device, isNewDevice)
      }
      }
    //documents.foreach(println)
    Save2GP.saveDStream(result, bGreenPlumSink, table)
  }

  /**
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
  }
}
