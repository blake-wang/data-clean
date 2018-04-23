package com

import java.util

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.process.agent.AgentProcess.FORMAT_STR
import org.bson.Document

import scala.collection.JavaConversions._

/**
  * Created by admin on 2018/3/8.
  */
object SdkClickTest {
  def main(args: Array[String]): Unit = {
    val log = "NORMAL: [2018-03-06 10:45:19] CIP[117.136.90.87] ANALYSIS[{\"device\":{\"system_version\":\"7.1.2\",\"device_id\":\"00000000-05ee-a682-ffff-ffff859b8101\",\"application_version\":3100,\"imei\":\"864699032248100\",\"screen_size\":\"1920|1080\",\"application_name\":\"青云诀\",\"sdk_version\":\"4.1.0\",\"device_name\":\"Xiaomi MI 5X\",\"from\":\"android\",\"imsi\":\"460028350317539\",\"package_name\":\"com.junhai.qyj.dalan\"},\"analysis_id\":\"A121U100000263\",\"event_record\":[{\"create_time\":1520304315,\"event\":\"sdk_log_second\",\"id\":\"2171\",\"user_name\":\"\"},{\"create_time\":1520304317,\"event\":\"sdk_log_second_in\",\"id\":\"2172\",\"user_name\":\"\"},{\"create_time\":1520304317,\"event\":\"sdk_log_second_in_success\",\"id\":\"2173\",\"user_name\":\"\"}]}]"
    var boolean = true
    val info =
      try {
        JunhaiLog.BsonParser(log, "ANALYSIS[")
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          boolean = false
          (new Document(), 0, "")
      }


    if (boolean) {
      val srcDoc = info._1
      val serverTs = info._2
      val doc = srcDoc.get("device").asInstanceOf[Document].append("analysis_id", srcDoc.getString("analysis_id"))
        .append("server_ts", serverTs).append("server_date_day", TimeUtil.time2DateString(FORMAT_STR, serverTs, TimeUtil.SECOND))
        .append("ip", info._3)
      val array: util.ArrayList[Document] = srcDoc.get("event_record").asInstanceOf[util.ArrayList[Document]]

      array.foreach(log=>{
        for (k <- doc.keySet()) {
          log.append(k, JunhaiLog.getString(doc, k))
        }
      })
      array.foreach(println(_))
    }
  }
}
