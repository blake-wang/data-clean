package com.ijunhai.process.jhchannel

import java.util.Date

import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.process.agent.KafkaLogProcess
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.MD5Util
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

/**
  * Created by Admin on 2017-11-16.
  */
object Doc2GPProcess {
  val CHUMENG_DATABASE = "chumeng_ad"
  val DALAN_DATABASE = "youyun_ad"
  val TOPIC2TABLE = Map("dlActive" -> "ad_active", "dlLogin" -> "ad_login", "dlOrder" -> "ad_pay", "dlUser" -> "ad_reg")
  val TABLE_DATA_FIELDS = Map("ad_active" -> Array("active_time"), "ad_login" -> Array("login_time"), "ad_reg" -> Array("reg_time"), "ad_pay" -> Array("pay_time"))

  def process(rDD: RDD[String], kafkaSink: Broadcast[KafkaSink], bGreenPlumSink: Broadcast[GreenPlumSink], topic: String): Unit = {
    Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Doc2GPProcess.getClass.getName, JunhaiLog.INFO, "Total get from kafka", "Num", rDD.count() + "")
    val table = TOPIC2TABLE.getOrElse(topic, "")
    val data_Fields: Array[String] = TABLE_DATA_FIELDS.getOrElse(table, Array(""))
    val allLogs = rDD.map(log => {
      try {
        (Document.parse(log), true)
      } catch {
        case e: Exception =>
          (new Document(KafkaLogProcess.LOG, log), false)
      }
    })

    val logs = allLogs.map(line => {
      //时间字段转换
      JunhaiLog.documentClean2GP(line._1.append("id", MD5Util.getMD5(line._1.toJson())), data_Fields)
    })

//    val result = Save2GreenPlum.saveDStreamTest(logs, bGreenPlumSink, table,kafkaSink)

    val result = Save2GP.saveDStream(logs, bGreenPlumSink, table)
    Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Doc2GPProcess.getClass.getName, JunhaiLog.INFO, "Total send to GP table " + table, "Num", result.count() + "")

  }
}
