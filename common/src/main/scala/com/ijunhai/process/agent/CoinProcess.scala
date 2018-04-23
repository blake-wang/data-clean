package com.ijunhai.process.agent

import com.ijunhai.common.CleanConstants._
import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.storage.kafka.KafkaSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.joda.time.DateTime

object CoinProcess {
  val error = "error"
  val game_id = "game_id"
  val channel_id = "channel_id"
  val game_channel_id = "game_channel_id"
  val user_id = "user_id"
  val role_id = "role_id"
  val role_name = "role_name"
  val server = "server"
  val _type = "type"
  val trade_count = "trade_count"
  val remain_count = "remain_count"
  val item_name = "item_name"
  val item_count = "item_count"
  val trade_time = "trade_time"
  val trade_desc = "trade_desc"
  val trade_date = "trade_date"

  def process(rdd: RDD[String], dataFormat: String, kafkaSink: Broadcast[KafkaSink], desc: String) {
    val sc = rdd.sparkContext
    //http://game.data.ijunhai.com/Gamedata/api/getAgentGameChannel
    val agcTuple = AgentProcess.getAgentGameChannel(hdfsCachePath)
    val bGgMap = sc.broadcast(agcTuple._1)
    val bGcMap = sc.broadcast(agcTuple._2)
    //http://game.data.ijunhai.com/Gamedata/api/getAgentSubChannel
    val bScMap = sc.broadcast(AgentProcess.getSubChannel(hdfsCachePath))
    //http://game.data.ijunhai.com/Gamedata/api/getAgentOldGameChannel
    val bBscMap = sc.broadcast(AgentProcess.getAllBySubChannel(hdfsCachePath))
    //http://bigdata.ijunhai.com/api/getBelongGameToGameChannelId
    //(doc.getString(belong_game_id), (doc.getString(game_channel_id), doc.getString(channel_id), doc.getString(game_id))
    val bBelongGame = sc.broadcast(AgentProcess.getAgent2GameChannel(hdfsCachePath))

    val doc =
      if (dataFormat.equals("json")) {
        rdd.map(str => {
          var boolean = true
          val data = if (str.contains("GAME_COIN_LOG[[")) {
            str.substring(str.indexOf("GAME_COIN_LOG[") + 15, str.length - 2)
          } else if (str.contains("GAME_COIN_LOG[")) {
            str.substring(str.indexOf("GAME_COIN_LOG") + 14, str.length - 1)
          } else {
            boolean = false
            str
          }
          (data.replaceAll("\\},\\{", "\\},,\\{").split(",,"), boolean)
        }).filter(_._2).flatMap(_._1).map(log => {
          try {
            Document.parse(log)
          } catch {
            case e: Exception =>
              e.printStackTrace()
              kafkaSink.value.send(desc + "Error", "json Document parse error ! source_data: " + log)
              new Document()
          }
        })
      } else {
        rdd.map(str => {
          var boolean = true
          val msgs = str.split('\t')
          if (msgs.length != 14) {
            boolean = false
            kafkaSink.value.send(desc + "Error", "csv msgs' length is less than 14 ! source_data: " + str)
            new Document()
          } else {
            new Document().append(game_id, msgs(0)).append(channel_id, msgs(1)).append(game_channel_id, msgs(2))
              .append(user_id, msgs(3)).append(role_id, msgs(4)).append(role_name, msgs(5)).append(server, msgs(6))
              .append(_type, msgs(7)).append(trade_count, msgs(8)).append(remain_count, msgs(9)).append(item_name, msgs(10))
              .append(item_count, msgs(11)).append(trade_desc, msgs(12)).append(trade_time, msgs(13))
          }
        })
      }

    val finalLog = doc.map(log => {
      val sourceLog = log
      var boolean = true
      if (!log.keySet().isEmpty || log != null ) {
        val gameId = JunhaiLog.getString(log, game_id)
        val channelId = JunhaiLog.getString(log, channel_id)
        val gameChannelId = JunhaiLog.getString(log, game_channel_id)

        if (gameId.equals("")){
          boolean = false
          kafkaSink.value.send(desc + "Error", dataFormat + "game_id is empty ! Source:" +log.toJson)
          //1. game_id为belong_game_id
        }else if (gameId.contains("SA") || bBelongGame.value.contains(gameId)) {
          val agentValues = bBelongGame.value.getOrElse(gameId, (error, error, error))
          log.remove(game_channel_id)
          log.remove(channel_id)
          log.remove(game_id)
          log.append(game_channel_id, agentValues._1).append(channel_id, agentValues._2).append(game_id, agentValues._3)
         //2. game_id 为独代的，且game_channel_id为2050，不做处理
        } else if (gameChannelId.equals("2050")) {
         //3. game_id 为独代的，且小于63
        } else if (Integer.parseInt(gameId) < 63) {
          log.remove(game_channel_id)
          //  3.1. 独代channel_id接口包含该channel_id,此时channel_id就是独代channel_id
          if (!bScMap.value.contains(channelId)) {
            val agentGameChannelId = bGcMap.value.getOrElse(gameId + channelId, error)
            log.put(game_channel_id, agentGameChannelId)
          //  3.2. 不包含该channel_id,此时channel_id是sub_channel,先获得独代channelId
          } else {
            val agentChannelId = bScMap.value.getOrElse(channelId, error)
            log.remove(channel_id)
            log.put(JunhaiLog.channel_id, agentChannelId)
            val value = bBscMap.value.getOrElse(channelId, error)
            if (value.equals(error)) { //若出现subchannelId匹配不到的情况，则直接使用独代的channelId去匹配得到gamechannelid
              val agentGameChannelId = bGcMap.value.getOrElse(gameId + agentChannelId, error)
              log.put(game_channel_id, agentGameChannelId)
            } else {
              log.put(game_channel_id, value)
            }
          }
        //4. game_id大于63，channel_id可能为空也可能不准，game_channel_id一定不为空，根据game_channel_id获得channel_id
        } else if (Integer.parseInt(gameId) >= 63) {
          val value = bGgMap.value.getOrElse(gameId + gameChannelId, error)
          log.put(channel_id, value)
        }
        log.append(trade_date, TimeUtil.time2DateString("yyyyMMdd", JunhaiLog.getLong(log, trade_time), TimeUtil.SECOND))
        if (log.containsValue(error)) {
          boolean = false
          kafkaSink.value.send(desc + "Error", dataFormat + " Before:" + sourceLog.toJson + ";After:" + log.toJson())
        }
        (log.toJson, boolean)
      } else {
        ("", false)
      }
    }).filter(_._2).map(_._1)

    if (dataFormat.equals("json")) {
      finalLog.foreach(log => kafkaSink.value.send(desc, log))
    } else {
      finalLog.saveAsTextFile("/user/hive/warehouse/org.db/coinlog/dt=" + new DateTime().minusDays(1).toLocalDateTime.toString("yyyyMMdd"))
    }
  }
}
