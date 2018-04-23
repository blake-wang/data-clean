package com.kafka

import java.text.SimpleDateFormat
import java.util.Locale

import com.ijunhai.common.{MD5Util, TimeUtil}
import com.ijunhai.process.jhchannel.ClickProcess
import com.ijunhai.storage.greenplum.GreenPlumSink
import org.bson.Document

/**
  * Created by admin on 2018/1/22.
  */
object pageAdv {
  def main(args: Array[String]): Unit = {
    val str = "219.133.46.70 - - [22/Jan/2018:00:20:42 +0800] " +
      "\"GET /collect.gif?act=click&url=http://res.moetouch.com/game/shjzbzcs_ad/apk_sem/bd_shjzbzcs_ad_1_306.apk&timestamp=1516594841000 HTTP/1.1\" 200 43 [0.000]" +
      "  \"http://page.moetouch.com/2018/play_0102/19654.html\"" +
      " \"Mozilla/5.0 (Linux; U; Android 7.1.1; zh-CN; MI 6 Build/NMF26X) AppleWebKit/537.36 (KHTML, like Gecko)" +
      " Version/4.0 Chrome/57.0.2987.108 UCBrowser/11.8.3.963 Mobile Safari/537.36\" \"61.154.11.235\""
    pageAdvLogAnalysis(str)
    val cmClickCount = pageGetCount(pageAdvLogAnalysis(str))
    val CmGreenPlumSink = GreenPlumSink.apply(ClickProcess.CM_DATABASE)
    CmGreenPlumSink.insert(cmClickCount, ClickProcess.PAGE_CLICK_DOWN_TABLE)
  }

  def pageAdvLogAnalysis(str: String): Document = {
      try {
        val nginx_time = str.substring(str.indexOf("[") + 1, str.indexOf("]"))
        val event = str.substring(str.indexOf("act=") + 4, str.indexOf("&url"))
        val apk = str.substring(str.indexOf("apk_sem/") + 8, str.indexOf(".apk"))
        val sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
        val transferFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val time = transferFormat.format(sourceFormat.parse(nginx_time))
        val doc = new Document
        doc.append(ClickProcess.click_time, time).append(ClickProcess.event_tag, event).append(ClickProcess.apk_name, apk)
        doc

    }
  }

  def pageGetCount(line: Document): Document = {
      val time = TimeUtil.dateString2Time("yyyy-MM-dd HH:mm", line.getString("click_time"), TimeUtil.SECOND)
      val date = TimeUtil.time2DateString("yyyyMMdd", time, TimeUtil.SECOND).toInt
      val hour = TimeUtil.time2DateString("HH", time, TimeUtil.SECOND).toInt
      val minute = TimeUtil.time2DateString("mm", time, TimeUtil.SECOND).toInt
      val eventTag = line.getString("event_tag")
      val apkName = line.getString("apk_name")
      val click = 50
      val document = new Document().append(ClickProcess.event_tag, eventTag).append("click_date", date)
        .append("click_hour", hour).append("click_minute", minute).append("click_cnt", click).append(ClickProcess.apk_name, apkName)
      val rowData = document.toJson + click
      document.put("id", MD5Util.getMD5(rowData))
      document
      }

}
