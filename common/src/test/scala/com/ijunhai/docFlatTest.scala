package com.ijunhai

import java.util

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.process.agent.KafkaLogProcess.{FORMAT_STR, error}
import org.bson.Document

import scala.collection.JavaConversions._

/**
  * Created by admin on 2018/2/5.
  */
object docFlatTest {
  def main(args: Array[String]): Unit = {
    val log = "NORMAL: [2018-03-07 20:02:25] CIP[117.136.88.166] ACTIVE_LOG[{\"extra_data\":{\"screen_size\":\"1920|1080\",\"device_id\":\"00000000-116c-28d1-44f0-e254100d13a9\",\"device_name\":\"360 1505-A01\",\"imsi\":\"861337039544535\",\"imei\":\"861337039544535\",\"system_version\":\"6.0\",\"sdk_version\":\"2.1\",\"package_name\":\"com.junhai.shjzbzcshd.zk\",\"application_name\":\"山海经之白泽传说HD\",\"application_version\":208},\"app_data\":{\"agent_game_id\":\"149\",\"game_channel_id\":\"104230\",\"action_type\":\"start\",\"time\":\"1520424142\"}}]"
    //    val document = Document.parse(str)
    //    val info = JunhaiLog.agentActiveLog(Str1)
    //    val appData = info.get("app_data").asInstanceOf[Document]
    //    val extraData = info.get("extra_data").asInstanceOf[Document]
    //    extraData.append("game_id",appData.getString("agent_game_id")).append("game_channel_id",appData.getString("game_channel_id"))
    //      .append("action_type",appData.getString("action_type")).append("time",appData.getString("time")).append("event","active")

    val info: ((Document, Int, String), Boolean) =
      try {
        (JunhaiLog.BsonParser(log,""), true)
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          ((new Document(), 0, ""), false)
      }
    if (info._2) {
      val srcDoc = info._1._1
      val serverTs = info._1._2
      val doc = srcDoc.get("app_data").asInstanceOf[Document]
      val extraData = srcDoc.get("extra_data").asInstanceOf[Document]
      for (k <- extraData.keySet()) {
        doc.append(k, JunhaiLog.getString(extraData, k))
      }
      val gameChannelId = try {
        JunhaiLog.getInt(doc, "game_channel_id")
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          0
      }
      //关联逻辑
      //game_id 小于63, game_channel_id 为belong_channel_id
      //game_id 大于等于63,根据game_channel_id 获得game_id和game_channel_id (因为game_id有可能不准)
      if (gameChannelId != 0) {
        val agentGameId = JunhaiLog.getInt(doc, "agent_game_id")

        val values: (String, String) =
          if (agentGameId < 63) {
            ("", agentGameId.toString)
          } else {
            val value = "111|555".split("\\|")
            (value(0), value(1))
          }

        doc.append(JunhaiLog.channel_id, values._1).append(JunhaiLog.game_id, values._2)
          .append(JunhaiLog.company_id, "").append(JunhaiLog.event, "active")
          .append(JunhaiLog.server_ts, serverTs).append("ip", info._1._3)
          .append(JunhaiLog.server_date + "_day", TimeUtil.time2DateString(FORMAT_STR, serverTs, TimeUtil.SECOND))
          .append(JunhaiLog.server_date + "_hour", TimeUtil.time2DateString("HH", serverTs, TimeUtil.SECOND))
          .append(JunhaiLog.server_date + "_minute", TimeUtil.time2DateString("mm", serverTs, TimeUtil.SECOND))
        doc.remove("agent_game_id")


        //                Save2Redis.isAgentNewDevice(doc, "agent", redisSinkCluster, "active")
        println(doc.toJson())
      }
    }
  }
}
