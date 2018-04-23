package com.ijunhai.batch

import com.ijunhai.storage.redis.RedisSink
import com.ijunhai.common.logsystem.JunhaiLog
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by admin on 2018/1/11.
  */
object TimeCorrect {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("TimeCorrect")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val redisSink = sc.broadcast(RedisSink.apply())

    val data = sc.textFile(args(0))
    val service = args(1)
    val event = args(2)
    val date = args(3)

    val document = data.map(log => {
      try {
        val doc = Document.parse(log)
        (doc, true)
      } catch {
        case e: Exception =>
          println(log)
          (new Document("log", log), false)
      }
    })

    document.map(doc => {
      val gameId = doc._1.get("game-game_id")
      val channelId = doc._1.get("agent-game_channel_id")
      val userId = doc._1.get("user-user_id")

      if (doc._1.getOrDefault(JunhaiLog.first_order_date, "").equals("")) {
        val key = service match {
          case "agent" => "agent_" + channelId + gameId + userId
          case "dalan" => "channel_dalan" + gameId + userId
          case "haiwai" => "channel_haiwai" + gameId + userId
        }


      }


    })
  }
}
