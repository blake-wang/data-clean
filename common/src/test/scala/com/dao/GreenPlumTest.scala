package com.dao

import java.util.Date

import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.process.jhchannel.ClickProcess._
import com.ijunhai.process.agent.AgentProcess
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document


object GreenPlumTest {
  def main(args: Array[String]): Unit = {
    val advDetail = "NORMAL: [2018-01-18 10:30:06] CIP[220.181.50.74] AD_CLICK[{\"junhai_adid\":\"26904\",\"game\":\"U200000146\",\"channel\":\"bdxxlzhitou\",\"device_id\":\"14a318ac3f99f2b76e6ec5226ecbf44c\",\"click_ip\":\"117.136.66.232\",\"callback_url\":\"http:\\/\\/als.baidu.com\\/cb\\/actionCb?a_type={{ATYPE}}&a_value={{AVALUE}}&ext_info=Sk71wQqHp4jZBfd%2Fpn00%2F2dtg%2BJT07WLVR%2B%2BQGQAm%2Bc1aY8CQhQtTi%2Bc%2BPoGADHUZJeRgzIIyMb4NtMDmNUAw2%2BH2DdGQJB3SU4Sbuyc%2BQ1rZxeKl6Y50Rv2vlfPdoA0Z0jLIrCTe%2FmcP7wzir10TVSSuwr0pPohAaIitVvvy3HVvvuKnGIeZp%2BhPpcASroXXQnKnkoakEBC1M2DJEoDK5upovf5JTO3aho%2FDpt%2FiupA%2BaeJtjDOTzb9l%2F5Az5S0fXV5cxcNUAxG1vKt09pR6hgpGw7Fu4kp7ydWf84n7ImMaDuqDvR%2FCwRdZ5ZFB1TBfmGGQ8NvmFbM3E%2B0Y52dRmiXjfEm9SzJtsWHjvV8zHhj1CAXu4iilVJ6O%2Fdqo5zkZka3q0h0n7UX4nrf2BT9gCqE%2FEehaCjShXxTo%2F0SCEprj05wHIpl5sAXRhZSGGq5\",\"click_time\":\"2018-01-18 10:30:06\",\"company_id\":\"1\"}]"
    val string = "NORMAL: [2018-01-18 10:30:13] CIP[117.136.63.187] AD_CLICK[{\"junhai_adid\":\"12130\",\"game\":\"U200000047\",\"click_time\":\"2018-01-18 10:30:13\",\"company_id\":\"1\"}]"
    val doc: (Document, Boolean) = advLogAnalysis(advDetail)

    println(doc._1.toJson)
    val gpDetail = getDetail2GP(doc._1)
    //    println(gpDetail.toJson)
    val greenPlumSink = GreenPlumSink.apply("click_detail")
    greenPlumSink.insert(gpDetail, "detail")
  }


  def getDetail2GP(document: Document): Document = new Document().append("id", MD5Util.getMD5(document.toJson())).append(game_, JunhaiLog.getString(document, game_))
    .append("channel", JunhaiLog.getString(document, "channel"))
    .append(junhai_adid, JunhaiLog.getString(document, junhai_adid))
    .append(client_ip, JunhaiLog.getString(document, client_ip))
    .append(company_id, JunhaiLog.getString(document, company_id))
    .append(click_time, TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss", JunhaiLog.getString(document, click_time)).asInstanceOf[java.sql.Date])
    .append("click_source", document.toJson)


  def advLogAnalysis(log: String): (Document, Boolean) = {
    val (time, ip, json) = getAdvContent(log)
    try {
      val doc = Document.parse(json)
      doc.put(client_ip, ip)
      if (!doc.containsKey(click_time)) {
        doc.put(click_time, time)
      }
      if (json.contains(company_id) && json.contains(junhai_adid)) {
        (doc, true)
      } else {
        (doc, false)
      }
    } catch {
      case e: Exception =>
        System.err.println(new Date() + " Error: json parse error!")
        (new Document(AgentProcess.LOG, json), false)
    }
  }
}
