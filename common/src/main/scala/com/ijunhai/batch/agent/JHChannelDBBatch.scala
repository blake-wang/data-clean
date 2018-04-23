package com.ijunhai.batch.agent

import java.util.Date

import com.ijunhai.common.CleanConstants._
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.process.agent.AgentProcess.orderTransform
import com.ijunhai.process.agent.{AgentProcess, KafkaLogProcess}
import com.ijunhai.process.saveFirstEvent
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by Admin on 2017-10-20.
  */
object JHChannelDBBatch {
  val ORDER_JH = "order_jh"
  val CHANNEL_OTHER_TABLES = "channel_other_tables"
  val ORDER = "order"
  val USER = "user"

  def main(args: Array[String]) {

    val rows = "rows"
    val table = "table"
    val game_id = "game_id"
    val jh_app_id = "jh_app_id"

    val sparkConf = new SparkConf()
      //      .setMaster("local")
      .setAppName("JunhaiChannelDatabaseBatch")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSink = sc.broadcast(RedisSink.apply())


    val system = args(0) //agent
    val useRedis = if (args(1) == "true") true else false //true 写入redis
    val data = sc.textFile(args(2)) //hdfs
    val outPutFile = if (args(3) == null) "" else args(3) //不写入hdfs则""
    val dataType = args(4)


    val gcgTuple = AgentProcess.getAgentGameChannelForChannel(hdfsCachePath)
    val bGcgMap = sc.broadcast(gcgTuple)
    val gcmMap = AgentProcess.getAgentGame(hdfsCachePath)
    val bGcmMap = sc.broadcast(gcmMap)
    val bGameChannel = sc.broadcast(AgentProcess.getAgent2GameChannel(hdfsCachePath, system))
    val (rmb, usd) = AgentProcess.getRate(hdfsCachePath)
    val bRmb = sc.broadcast(rmb)
    val bUsd = sc.broadcast(usd)
    val hbaseSink = sc.broadcast(HbaseSink.apply())


    val document = data.map(string => {
      try {
        val doc = Document.parse(string)
        (doc, true)
      } catch {
        case e: Exception =>
          System.err.println(new Date() + " Error: json parse error!")
          (new Document("log", string), false)
      }
    })

    if (dataType == "order" || dataType == "user") {
      val orderDocument = system match {
        case "haiwai" =>
          orderTransform(document.filter(_._2).map(_._1), system, game_id, bGcgMap, bGameChannel, bGcmMap).map(line => {
            val document = line
            val order = JunhaiLog.getDocument(document, JunhaiLog.order)
            val currencyAmount = JunhaiLog.getDouble(order, JunhaiLog.currency_amount)
            val currencyType = JunhaiLog.getString(order, JunhaiLog.currency_type)

            val rmb = bRmb.value.getOrElse(currencyType, 1.0) * currencyAmount
            val usd = bUsd.value.getOrElse(currencyType, 1.0) * currencyAmount
            order.append(JunhaiLog.cny_amount, rmb)
            order.append(JunhaiLog.usd_amount, usd)
            document
          })
        case _ =>
          orderTransform(document.filter(_._2).map(_._1), system, game_id, bGcgMap, bGameChannel, bGcmMap)
      }

      val resultDocuments: RDD[String] = orderDocument.union(AgentProcess.registerTransform(document.filter(_._2).map(_._1), jh_app_id, bGameChannel, bGcmMap))
        .map(document => {
          if (useRedis){
            KafkaLogProcess.documentFlat("", saveFirstEvent.save(document.append(JunhaiLog.order_step,"paid_success"),hbaseSink,redisSink))
          }else{
            KafkaLogProcess.documentFlat("", saveFirstEvent.saveBatch(document,hbaseSink))
          }
        document.toJson
        })
      if (outPutFile != "") {
        resultDocuments.saveAsTextFile(outPutFile)
      } else {
        resultDocuments.count()
      }
    } else if (dataType == "order_jh") {
      val order_jh = AgentProcess.orderTransform(document.filter(_._2).map(_._1), system, game_id, bGcgMap, bGameChannel, bGcmMap, false) //最后一个标识为false表示使用jh_channel来关联。
      val orderWithNewUserFlag = Save2Redis.saveDStream(order_jh, system, redisSink)
        .map(document => {
          KafkaLogProcess.documentFlat("", document)
        })
      orderWithNewUserFlag.map(_.toJson()).saveAsTextFile(outPutFile)
      //        Save2Kafka.saveDStreamSync(orderWithNewUserFlag.map(_.toJson()),kafkaSinkBroadcast,ORDER_JH,8,JunhaiChannelDatabaseBatch.getClass.getName)
    }
    //    Save2Kafka.saveDStreamSync(otherTableData.map(_._1.toJson()),CHANNEL_OTHER_TABLES,8)
  }
}
