package com.ijunhai.batch.process

import java.util
import java.util.Date

import com.ijunhai.common.CleanConstants._
import com.ijunhai.common.HDFSUtil
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.JunhaiLog._
import com.ijunhai.common.logsystem.{JunhaiLog, Monitor}
import com.ijunhai.common.serde.MessageSerde
import com.ijunhai.process.agent.AgentProcess
import com.ijunhai.process.agent.KafkaLogProcess.documentFlat
import com.ijunhai.process.saveFirstEvent
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
import org.apache.rocketmq.common.message.MessageExt
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document
import com.ijunhai.process._

import scala.collection.JavaConversions._


object CommonProcessBatch extends Serializable {


  def loginProcess(rdd: RDD[String], source: String,
                   redisSink: Broadcast[RedisSink],
                   kafkaSink: Broadcast[KafkaSink],
                   gpSink: Broadcast[GreenPlumSink],
                   hBaseSink: Broadcast[HbaseSink], useRedis: Boolean): Unit = {
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
        val (afterFormat, booleanA) = try {
          val doc = Document.parse(log)
          (doc, true)
        } catch {
          case _: Exception =>
            (new Document("log", log), false)
        }

        val (afterJoin, booleanB) = Process.loginJoin(afterFormat, source, bAcMap, bGgMap, bGcMap, bScMap, bBscMap, bGcmMap, bGameChannel, bRmb, bUsd, bIOS128, booleanA)
        val booleanC = if(useRedis) {
          saveFirstEvent.saveAll(afterJoin, hBaseSink, redisSink, booleanB)
        }else{
          saveFirstEvent.saveBatch(afterJoin, hBaseSink)
          booleanB
        }

        val (finalLog, topicName) = if (booleanC) {
          val afterFlat = documentFlat("", afterJoin).toJson()
          (afterFlat,
            source match {
              case JunhaiLog.agentLoginSrc =>
                if (bIOS128.value.contains(JunhaiLog.getSecondColumnString(afterJoin, JunhaiLog.agent, game_channel_id))) {
                  otherAcc.add(1L)
                  dest + OTHER
                } else {
                  destAcc.add(1L)
                  kafkaSink.value.send(dest + "Batch", afterFlat)
                  dest
                }
              case JunhaiLog.dalanLoginSrc =>
                destAcc.add(1L)
                kafkaSink.value.send("agentLoginBatch", afterFlat)
                "agentLogin"

              case JunhaiLog.haiwaiLoginSrc =>
                destAcc.add(1L)
                kafkaSink.value.send(dest + "Batch", afterFlat)
                dest
            })
        } else {
          filterAcc.add(1L)
          (log, dest + FILTER)
        }
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
    Save2GP.monitor(gpSink, source, map, "monitor_batch")
  }


  def orderProcess(rdd: RDD[String], source: String,
                   redisSink: Broadcast[RedisSink],
                   kafkaSink: Broadcast[KafkaSink],
                   gpSink: Broadcast[GreenPlumSink],
                   hBaseSink: Broadcast[HbaseSink], useRedis: Boolean): Unit = {
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


    val tableName = order

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
      try {
        val doc = Document.parse(log)
        (doc, true)
      } catch {
        case e: Exception =>
          System.err.println(new Date() + " Error: json parse error!")
          (new Document("log", log), false)
      }
    }).foreachPartition(p => {
      val ipClass = new IP
      ipClass.load(bBytes.value)
      p.foreach(msg => {
        srcAcc.add(1L)
        val (log, boolean) = msg
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
                     if(useRedis) {
                      saveFirstEvent.saveAll(afterJoin.append(JunhaiLog.status, "2"), hBaseSink, redisSink)
                    }else{
                      saveFirstEvent.saveBatch(afterJoin, hBaseSink)
                    }

                    destAcc.add(1L)
                    kafkaSink.value.send(dest + "Batch", afterJoin.toJson())
                    dest

                  }
              })
          case (_, "order") =>
            val (afterJoin, booleanA) = Process.orderJoin(log, source, bAcMap, bGgMap, bGcMap, bScMap, bBscMap, bGcmMap, bGameChannel, bRmb, bUsd, bIOS128, bGcgMap)
            val booleanB =

            if(useRedis) {
              saveFirstEvent.saveAll(afterJoin.append(JunhaiLog.status, "2"), hBaseSink, redisSink, booleanA)
            }else{
              saveFirstEvent.saveBatch(afterJoin, hBaseSink)
              booleanA
            }


            (afterJoin, if (booleanB) {
              destAcc.add(1L)
              kafkaSink.value.send((if (source.contains(JunhaiLog.serviceDalan)) "agentDB" else dest) + "Batch", afterJoin.toJson())
              if (source.contains(JunhaiLog.serviceDalan)) "agentDB" else dest
            } else {
              filterAcc.add(1L)
              dest + FILTER
            })
          case (_, _) => //包括order_jh/user 都过滤掉
            filterAcc.add(1L)
            (new Document(tableName, log), dest + FILTER)
        }
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
    Save2GP.monitor(gpSink, source, map, "monitor_batch")
  }
}
