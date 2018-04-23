package com.ijunhai.batch.agent

import java.util.Date

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.process.agent.AgentProcess
import com.ijunhai.process.agent.AgentProcess.getIOS128
import com.ijunhai.process.saveFirstEvent
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import com.ijunhai.common.CleanConstants._


object OrderJoinBatch {

  def main(args: Array[String]) {

    val game_id = "game_id"
    val game_channel_id = "game_channel_id"
    val channel_id = "channel_id"
    val create_ip = "create_ip"
    val sub_pf = "sub_pf"
    val error = "error"

    val sparkConf = new SparkConf()
      //      .setMaster("local")
      .setAppName("OrderJoinBatch")
    val sc = new SparkContext(sparkConf)

    val system = args(0) //agent
    val useRedis = if (args(1) == "true") true else false //true 写入redis
    val rdd1 = sc.textFile(args(2)) //hdfs
    val outPutFile = if (args(3) == null) "" else args(3) //不写入hdfs则""

    val agcTuple = AgentProcess.getAgentGameChannel(hdfsCachePath)
    val acMap = AgentProcess.getAgentChannel(hdfsCachePath)
    val gcmMap = AgentProcess.getAgentGame(hdfsCachePath)
    val scMap = AgentProcess.getSubChannel(hdfsCachePath)
    val bscMap = AgentProcess.getAllBySubChannel(hdfsCachePath)
    val ggMap = agcTuple._1
    val gcMap = agcTuple._2

    val bAcMap = sc.broadcast(acMap)
    val bGgMap = sc.broadcast(ggMap)
    val bGcMap = sc.broadcast(gcMap)
    val bScMap = sc.broadcast(scMap)
    val bBscMap = sc.broadcast(bscMap)
    val bGcmMap = sc.broadcast(gcmMap)

    val iOS128 = getIOS128(hdfsCachePath)
    val bIOS128 = sc.broadcast(iOS128)


    val kafkaSinkBroadcast: Broadcast[KafkaSink] = sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSink = sc.broadcast(RedisSink.apply())
    val hbaseSink = sc.broadcast(HbaseSink.apply())

    val document = rdd1.map(string => {
      try {
        val doc = Document.parse(string)
        (doc, true)
      } catch {
        case _: Exception =>
          System.err.println(new Date() + " Error: json parse error!")
          (new Document("log", string), false)
      }
    })

    val resultDocuments = document.map(order => {
      //        if(bCopyMap.value.nonEmpty){
      //          order._1.append(create_ip,bCopyMap.value.get(orderSn,"0.0.0.0"))
      //        }
      //开始关联
      val gameId = order._1.getString(game_id).toInt
      order._1.put(game_id, gameId)
      order._1.put("server", order._1.getString("server").toInt)
      order._1.put("goods_count", order._1.getString("goods_count").toInt)
      order._1.put("pay_status", order._1.getString("pay_status").toInt)
      order._1.put("money", order._1.getString("money").toDouble)
      order._1.put("id", order._1.getString("id").toInt)
      order._1.put("status", order._1.getString("status").toInt)

      val columnValue = order._1.getString("channel_id") //默认的独代的channel_id字段
      if (gameId >= 63) {
        //此时order表中的channel_id存放的是game_channel_id的值 需要得到channel_id
        val value = bGgMap.value.getOrElse(gameId + columnValue, error)
        val subPf = bAcMap.value.getOrElse(value, error)
        order._1.put(channel_id, value)
        order._1.put(game_channel_id, columnValue) //将原来的字段名字改回来
        order._1.put(sub_pf, subPf)
      } else {
        if (!bScMap.value.contains(columnValue)) {
          //此时order表中的channel_id就是channel_id
          val value = bGcMap.value.getOrElse(gameId + columnValue, error) //拿到game_channel_id
          val subPf = bAcMap.value.getOrElse(columnValue, error)
          order._1.put(game_channel_id, value)
          order._1.put(sub_pf, subPf)
        } else {
          val value = bBscMap.value.getOrElse(columnValue, error)
          //根据sub_channel拿game_channel_id
          val channelId = bScMap.value.getOrElse(columnValue, error)
          order._1.put(channel_id, channelId)
          if (value.equals(error)) {
            val agentValue = bGcMap.value.getOrElse(gameId + channelId, error)
            order._1.put(JunhaiLog.game_channel_id, agentValue)
          } else {
            order._1.put(JunhaiLog.game_channel_id, value)
          }
          val subPf = bAcMap.value.getOrElse(channelId, error)
          order._1.put(sub_pf, subPf)
        }
      }
      val companyId = bGcmMap.value.getOrElse(gameId.toString, error) //游戏公司关联
      order._1.put(JunhaiLog.company_id, companyId)
      order._1
    })
    val agentWithoutDalan = resultDocuments.filter { case bson =>
      //剔除大蓝渠道的数据
      val channelId = bson.get(JunhaiLog.channel_id)
      channelId match {
        case i: Integer =>
          bson.put(JunhaiLog.channel_id, channelId.toString)
        case i: String =>
      }
      !JunhaiLog.junhaiChannelsId.contains(bson.getString(JunhaiLog.channel_id)) //这里把大蓝的都踢掉了
    }

    val withNewUserFlag = agentWithoutDalan.map(log => {
      if (useRedis) {
        saveFirstEvent.save(log.append(JunhaiLog.status,"2"), hbaseSink, redisSink, true)
      } else {
        saveFirstEvent.saveBatch(log, hbaseSink, true)
      }
    })

    if (outPutFile != "") {
      withNewUserFlag.cache()
      withNewUserFlag.filter((line: Document) => {
        bIOS128.value.contains(line.get(game_channel_id).toString)
      }).map(_.toJson()).saveAsTextFile(outPutFile + "Other")
      withNewUserFlag.filter((line: Document) => {
        !bIOS128.value.contains(line.get(game_channel_id).toString)
      }).map(_.toJson()).saveAsTextFile(outPutFile)
      withNewUserFlag.unpersist()
    } else {
      withNewUserFlag.count()
    }


  }
}
