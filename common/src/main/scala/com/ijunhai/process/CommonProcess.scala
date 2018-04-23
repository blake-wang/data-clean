package com.ijunhai.process

import java.util
import java.util.Date
import com.ijunhai.common.logsystem.JunhaiLog._
import com.ijunhai.common.CleanConstants._
import com.ijunhai.common.HDFSUtil
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.{JunhaiLog, Monitor}
import com.ijunhai.common.serde.MessageSerde
import com.ijunhai.process.agent.AgentProcess
import com.ijunhai.process.agent.KafkaLogProcess.documentFlat
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
import org.apache.rocketmq.common.message.MessageExt
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConversions._

object CommonProcess extends Serializable {


  def loginProcess(rdd: RDD[String], source: String,
                   redisSink: Broadcast[RedisSink],
                   kafkaSink: Broadcast[KafkaSink],
                   gpSink: Broadcast[GreenPlumSink],
                   hBaseSink: Broadcast[HbaseSink]): Unit = {
    val sparkContext = rdd.sparkContext
    val cleanConf: Broadcast[Document] = sparkContext.broadcast(HDFSUtil.readConfigFromHdfs(configPath))
    val bBytes = sparkContext.broadcast(IPAddress.init(ipDatabasePath))
    val agcTuple = AgentProcess.getAgentGameChannel(hdfsCachePath)
    val bAcMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getAgentChannel(hdfsCachePath))
    val bGgMap: Broadcast[Map[String, String]] = sparkContext.broadcast(agcTuple._1)
    val bGcMap: Broadcast[Map[String, String]] = sparkContext.broadcast(agcTuple._2)
    val bScMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getSubChannel(hdfsCachePath))
    val bBscMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getAllBySubChannel(hdfsCachePath))
    val bGcmMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getAgentGame(hdfsCachePath))
    val bGameChannel: Broadcast[Map[String, (String, String, String)]] = sparkContext.broadcast(AgentProcess.getAgent2GameChannel(hdfsCachePath, source))
    val (rmb, usd) = AgentProcess.getRate(hdfsCachePath)
    val bRmb: Broadcast[Map[String, Double]] = sparkContext.broadcast(rmb)
    val bUsd: Broadcast[Map[String, Double]] = sparkContext.broadcast(usd)
    val bIOS128: Broadcast[Set[String]] = sparkContext.broadcast(AgentProcess.getIOS128(hdfsCachePath))

    val srcAcc = sparkContext.longAccumulator
    val destAcc = sparkContext.longAccumulator
    val otherAcc = sparkContext.longAccumulator
    val filterAcc = sparkContext.longAccumulator
    val dest = source.substring(0, source.length - 3)

    rdd.foreachPartition(p => {
      val ipClass = new IP
      ipClass.load(bBytes.value)
      p.foreach(log => {
        srcAcc.add(1L)
        val (afterFormat, booleanA) = Process.loginFormat(log, source, cleanConf, ipClass)
        val (afterJoin, booleanB) = Process.loginJoin(afterFormat, source, bAcMap, bGgMap, bGcMap, bScMap, bBscMap, bGcmMap, bGameChannel, bRmb, bUsd, bIOS128, booleanA)
        val booleanC = saveFirstEvent.saveAll(afterJoin, hBaseSink, redisSink, booleanB)
        val (finalLog, topicName) = if (booleanC) {
          (documentFlat("", afterJoin).toJson(),
            source match {
              case JunhaiLog.agentLoginSrc =>
                if (bIOS128.value.contains(JunhaiLog.getSecondColumnString(afterJoin, JunhaiLog.agent, game_channel_id))) {
                  otherAcc.add(1L)
                  dest + OTHER
                } else {
                  destAcc.add(1L)
                  dest
                }
              case JunhaiLog.dalanLoginSrc =>
                destAcc.add(1L)
                "agentLogin"
              case JunhaiLog.haiwaiLoginSrc =>
                destAcc.add(1L)
                dest
            })
        } else {
          filterAcc.add(1L)
          (log, dest + FILTER)
        }
        kafkaSink.value.send(topicName, finalLog)
      })
    })

    val map = new util.HashMap[String, Int]()
    map.put(source, srcAcc.value.toInt)
    map.put(dest + FILTER, filterAcc.value.toInt)
    source match {
      case JunhaiLog.agentLoginSrc =>
        map.put(dest, destAcc.value.toInt)
        map put(dest + OTHER, otherAcc.value.toInt)
      case JunhaiLog.dalanLoginSrc =>
        map.put("agentLogin", destAcc.value.toInt)
      case _ =>
        map.put(dest, destAcc.value.toInt)
    }
    Save2GP.monitor(gpSink, source, map)
  }


  def orderProcess(rdd: RDD[String], source: String,
                   redisSink: Broadcast[RedisSink],
                   kafkaSink: Broadcast[KafkaSink],
                   gpSink: Broadcast[GreenPlumSink],
                   hBaseSink: Broadcast[HbaseSink]): Unit = {
    val sparkContext: SparkContext = rdd.sparkContext
    val bBytes = sparkContext.broadcast(IPAddress.init(ipDatabasePath))
    val agcTuple = AgentProcess.getAgentGameChannel(hdfsCachePath)
    val bGcgMap = sparkContext.broadcast(AgentProcess.getAgentGameChannelForChannel(hdfsCachePath))
    val bAcMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getAgentChannel(hdfsCachePath))
    val bGgMap: Broadcast[Map[String, String]] = sparkContext.broadcast(agcTuple._1)
    val bGcMap: Broadcast[Map[String, String]] = sparkContext.broadcast(agcTuple._2)
    val bScMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getSubChannel(hdfsCachePath))
    val bBscMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getAllBySubChannel(hdfsCachePath))
    val bGcmMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getAgentGame(hdfsCachePath))
    val bGameChannel: Broadcast[Map[String, (String, String, String)]] = sparkContext.broadcast(AgentProcess.getAgent2GameChannel(hdfsCachePath, source))
    val (rmb, usd) = AgentProcess.getRate(hdfsCachePath)
    val bRmb: Broadcast[Map[String, Double]] = sparkContext.broadcast(rmb)
    val bUsd: Broadcast[Map[String, Double]] = sparkContext.broadcast(usd)
    val bIOS128: Broadcast[Set[String]] = sparkContext.broadcast(AgentProcess.getIOS128(hdfsCachePath))
    val filterRule: Broadcast[Map[String, (util.ArrayList[String], util.ArrayList[Document])]] = sparkContext.broadcast(AgentProcess.getFilterRule(hdfsCachePath))

    val srcAcc = sparkContext.longAccumulator
    val destAcc = sparkContext.longAccumulator
    val otherAcc = sparkContext.longAccumulator
    val copyAcc = sparkContext.longAccumulator
    val testAcc = sparkContext.longAccumulator
    val filterAcc = sparkContext.longAccumulator
    val srcBefore = srcAcc.value
    val destBefore = destAcc.value
    val copyBefore = copyAcc.value
    val testBefore = testAcc.value
    val otherBefore = otherAcc.value
    val filterBefore = filterAcc.value
    val dest = source.substring(0, source.length - 3)
    rdd.map(log => {
      if (!source.contains(DALAN)) {
        Process.getOrderData(log)
      } else {
        try {
          (Document.parse(log), true)
        } catch {
          case _: Exception =>
            (new Document().append("msg", log), false)
        }
      }
    }).filter(_._2).flatMap(line => {
      line._1.get("rows").asInstanceOf[util.ArrayList[Document]].map(doc => {
        (doc.get("data").asInstanceOf[Document], doc.getString("table"))
      })
    }).foreachPartition(p => {
      val ipClass = new IP
      ipClass.load(bBytes.value)
      p.foreach(msg => {
        srcAcc.add(1L)
        val (log, tableName) = msg
        Process.addRegion(log, ipClass)
        val (finalDoc, topicName) = (source, tableName) match {
          case (JunhaiLog.agentDBSrc, _) =>
            (log.append(JunhaiLog.order_sn, JunhaiLog.getString(log, JunhaiLog.order_sn)), //解决"order_sn" : { "$numberLong" : "2018041734992210113" }问题
              tableName match {
                case "agent_order_copy" =>
                  copyAcc.add(1L)
                  dest + COPY
                case _ =>
                  val (afterJoin, boolean) = Process.orderJoin(log, source, bAcMap, bGgMap, bGcMap, bScMap, bBscMap, bGcmMap, bGameChannel, bRmb, bUsd, bIOS128, bGcgMap)
                  if (!boolean || JunhaiLog.haiwaiChannelId.contains(JunhaiLog.getString(afterJoin, JunhaiLog.channel_id))) {
                    filterAcc.add(1L)
                    dest + FILTER
                  } else if (Process.isTestOrder(afterJoin, filterRule)) {
                    testAcc.add(1L)
                    dest + TEST
                  } else if (bIOS128.value.contains(JunhaiLog.getString(afterJoin, JunhaiLog.game_channel_id))) {
                    otherAcc.add(1L)
                    dest + OTHER
                  } else {
                    saveFirstEvent.saveAll(afterJoin, hBaseSink, redisSink)
                    destAcc.add(1L)
                    dest
                  }
              })
          case (_, "order") =>
            val (afterJoin, booleanA) = Process.orderJoin(log, source, bAcMap, bGgMap, bGcMap, bScMap, bBscMap, bGcmMap, bGameChannel, bRmb, bUsd, bIOS128, bGcgMap)
            val booleanB = saveFirstEvent.saveAll(afterJoin, hBaseSink, redisSink, booleanA)
            (afterJoin, if (booleanB) {
              destAcc.add(1L)
              if (source.contains(JunhaiLog.serviceDalan)) "agentDB" else dest
            } else {
              filterAcc.add(1L)
              dest + FILTER
            })
          case (_, _) => //包括order_jh/user 都过滤掉
            filterAcc.add(1L)
            (new Document(tableName, log),dest + FILTER)
        }
        kafkaSink.value.send(topicName, finalDoc.toJson())
      })
    })
    val map = new util.HashMap[String, Int]()
    map.put(source, (srcAcc.value - srcBefore).toInt)
    map.put(dest + FILTER, (filterAcc.value - filterBefore).toInt)
    source match {
      case JunhaiLog.agentDBSrc =>
        map.put(dest, (destAcc.value - destBefore).toInt)
        map put(dest + OTHER, (otherAcc.value - otherBefore).toInt)
        map.put(dest + COPY, (copyAcc.value - copyBefore).toInt)
        map.put(dest + TEST, (testAcc.value - testBefore).toInt)
      case JunhaiLog.dalanDBSrc =>
        map.put("agentDB", (destAcc.value - destBefore).toInt)
      case _ =>
        map.put(dest, (destAcc.value - destBefore).toInt)
    }
    Save2GP.monitor(gpSink, source, map)
  }

  def extraOnly(rdd: RDD[MessageExt]): RDD[String] = {
    rdd.filter(_.getBody != null).map(msge => {
      try {
        val wrapMessage = MessageSerde.deserialize(msge)
        val msgs = new String(wrapMessage.getMessageBody, "UTF-8").split('\n')
        val array = new Array[String](msgs.length)
        for (i <- msgs.indices) {
          array(i) = msgs(i)
        }
        (array, true)
      } catch {
        case e: Exception =>
          (Array(new String(msge.getBody, "UTF-8")), false)
      }
    }).flatMap(_._1)
  }


  def extraCoin(rdd: RDD[MessageExt]): RDD[String] = {
    val msgsWithInfo: RDD[(Array[(String, String)], Int, Int, String, String, String)] =
      rdd.filter(_.getBody != null).map(msge => {
        try {
          val wrapMessage = MessageSerde.deserialize(msge)
          val msgs = new String(wrapMessage.getMessageBody, "UTF-8").split('\n')
          val array = new Array[(String, String)](msgs.length)
          val lineCount = wrapMessage.getLineCount
          val length = msgs.length
          val dataSystem = wrapMessage.getDataSystem
          val filePath = if (wrapMessage.getFilePath == null) "" else wrapMessage.getFilePath
          val ip = if (wrapMessage.getIp == null) "" else wrapMessage.getIp
          for (i <- msgs.indices) {
            array(i) = (msgs(i), dataSystem)
          }
          (array, lineCount, length, filePath, ip, dataSystem)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            (Array((new Date() + ":" + new String(msge.getBody, "UTF-8"), "mqPullError")), 0, 0, "", "", "")
        }
      })

    msgsWithInfo.flatMap(_._1).filter(!_._2.equals("")).map(_._1) //过滤collect_metric数据
  }


  //从MQ解压
  def extraData(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink], nameServer: String, topicsStr: String): RDD[(String, String)] = {
    val msgsWithInfo: RDD[(Array[(String, String)], Int, Int, String, String, String)] =
      rdd.filter(_.getBody != null).map(msge => {
        try {
          val wrapMessage = MessageSerde.deserialize(msge)
          val msgs = new String(wrapMessage.getMessageBody, "UTF-8").split('\n')
          val array = new Array[(String, String)](msgs.length)
          val lineCount = wrapMessage.getLineCount
          val length = msgs.length
          val dataSystem = wrapMessage.getDataSystem
          val filePath = if (wrapMessage.getFilePath == null) "" else wrapMessage.getFilePath
          val ip = if (wrapMessage.getIp == null) "" else wrapMessage.getIp
          for (i <- msgs.indices) {
            array(i) = (msgs(i), dataSystem)
          }
          (array, lineCount, length, filePath, ip, dataSystem)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            (Array((new Date() + ":" + new String(msge.getBody, "UTF-8"), "mqPullError")), 0, 0, "", "", "")
        }
      })
    msgsWithInfo.map(line => (line._1, line._2, line._3, line._4, line._5, line._6)).foreach {
      case (array, lineCount, length, filePath, ip, dataSystem) => {
        if (!filePath.equals("") && !ip.equals("")) {
          //过滤collect_metric数据,保存原始数据的metric信息
          Save2Kafka.monitor(kafkaSink, dataSystem, ip, Save2Kafka.receivedFromMQ, length, Save2Kafka.logFile, filePath)
        } else {
          if (array.length > 0 && topicsStr.contains("collect_metric")) {
            for (i <- array.indices) {
              if (!array(i)._1.contains("\"value\":0")) {
                kafkaSink.value.syncSend("metricMonitor", array(i)._1) //保存collect_metric数据
              }
            }
          }
        }
      }
    }
    val a: RDD[(String, String)] = msgsWithInfo.flatMap(_._1)

    a.filter(!_._2.equals("")) //过滤collect_metric数据
  }

  def extra(rdd: RDD[MessageExt], GPSink: Broadcast[GreenPlumSink], nameServer: String, topicsStr: String): RDD[(String, String)] = {
    val msgsWithInfo =
      rdd.filter(_.getBody != null).map(msge => {
        try {
          val wrapMessage = MessageSerde.deserialize(msge)
          val msgs = new String(wrapMessage.getMessageBody, "UTF-8").split('\n')
          val array = new Array[(String, String)](msgs.length)
          val lineCount = wrapMessage.getLineCount
          val length = msgs.length
          val dataSystem = wrapMessage.getDataSystem
          val filePath = if (wrapMessage.getFilePath == null) "" else wrapMessage.getFilePath
          val ip = if (wrapMessage.getIp == null) "" else wrapMessage.getIp
          for (i <- msgs.indices) {
            array(i) = (msgs(i), dataSystem)
          }
          (array, lineCount, length, filePath, ip, dataSystem)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            (Array((new Date() + ":" + new String(msge.getBody, "UTF-8"), "mqPullError")), 0, 0, "", "", "")
        }
      })
    val time = new Date().getTime
    msgsWithInfo.map(line => (line._1, line._3, line._4, line._5, line._6)).foreach {
      case (array, length, filePath, ip, dataSystem) => {

        if (filePath.equals("") && ip.equals("") && array.length > 0 && topicsStr.contains("collect_metric")) {
          for (i <- array.indices) {
            if (!array(i)._1.contains("\"value\":0")) {
              Save2GP.collectMonitor(GPSink, array(i)._1) //保存collect_metric数据
            }
          }
        } else if (!filePath.equals("") && !ip.equals("")) {
          //过滤collect_metric数据,保存原始数据的metric信息
          //GPSink: Broadcast[GreenPlumSink],dataSource: String,serverName: String,metricName: String,value: Int ,
          // type: String,timestamp: Long,fileName: String = "",srcOrDec: String = "",tag: String = ""
          Save2GP.metricMonitor(GPSink, topicsStr.split(" ")(0), ip, Monitor.receivedFromMQ, length, Monitor.logFile, time, filePath)
        }
      }
    }
    msgsWithInfo.flatMap(_._1).filter(!_._2.equals("")) //过滤collect_metric数据
  }

  def extraDataWithCollectTime(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink], nameServer: String, topicsStr: String): (RDD[(String, String)], Set[String]) = {
    val msgsWithInfo =
      rdd.filter(_.getBody != null).map(msge => {
        try {
          val wrapMessage = MessageSerde.deserialize(msge)
          val msgs = new String(wrapMessage.getMessageBody, "UTF-8").split('\n')
          val lineCount = wrapMessage.getLineCount
          val length = msgs.length
          val filePath = if (wrapMessage.getFilePath == null) "" else wrapMessage.getFilePath
          val ip = if (wrapMessage.getIp == null) "" else wrapMessage.getIp
          val array = new Array[(String, String)](msgs.length)
          for (i <- msgs.indices) {
            array(i) = (msgs(i), wrapMessage.getDataSystem)
          }
          (array, lineCount, length, filePath, ip)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            (Array((new Date() + ":" + new String(msge.getBody, "UTF-8"), "mqPullError")), 0, 0, "", "")
        }
      })
    val ips: Set[String] = msgsWithInfo.map(line => {
      line._5
    }).collect.toSet
    msgsWithInfo.map(line => (line._2, line._3, line._4, line._5)).foreach {
      case (lineCount, length, filePath, ip) => {
        Save2Kafka.monitor(kafkaSink, topicsStr, ip, Save2Kafka.receivedFromMQ, lineCount, Save2Kafka.logFile, filePath)
      }
    }
    (msgsWithInfo.flatMap(_._1), ips)
  }

  def extraDataWithCollectTimeTest(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink],
                                   nameServer: String, topic: String): RDD[(String, String)] = {
    val msgsWithInfo: RDD[(Array[(String, String)], Int, Int, String, String)] =
      rdd.filter(_.getBody != null).map(msge => {
        try {
          val wrapMessage = MessageSerde.deserialize(msge)
          val msgs = new String(wrapMessage.getMessageBody, "UTF-8").split('\n')
          val lineCount: Int = wrapMessage.getLineCount
          val length = msgs.length
          val filePath = if (wrapMessage.getFilePath == null) "" else wrapMessage.getFilePath
          val ip = if (wrapMessage.getIp == null) "" else wrapMessage.getIp

          val array = new Array[(String, String)](msgs.length)
          for (i <- msgs.indices) {
            array(i) = (msgs(i), wrapMessage.getDataSystem)
          }
          (array, lineCount, length, filePath, ip)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            (Array((new Date() + ":" + new String(msge.getBody, "UTF-8"), "mqPullError")), 0, 0, "", "")
        }
      })


    msgsWithInfo.map(line => (line._2, line._3, line._4, line._5)).foreach {
      case (lineCount, length, filePath, ip) => {
        Save2Kafka.monitor(kafkaSink, topic, ip, Save2Kafka.receivedFromMQ, lineCount, Save2Kafka.logFile, filePath)
      }
    }

    msgsWithInfo.flatMap(_._1)
  }

}
