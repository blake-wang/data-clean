package com.ijunhai.common.logsystem

import com.ijunhai.common.{MD5Util, TimeUtil}
import java.util.UUID
import org.bson.Document

/**
  * Created by Admin on 2017-11-09.
  */
class Monitor {
  var dataSource = ""
  var fileName = ""
  var host = ""
  var metricName = ""
  var tags = ""
  var time = TimeUtil.getDefaultSqlDate
  var monitorType = ""
  var value = 0
  var topic = ""
  var id = ""
}

object Monitor {
  var database = "monitor"

  var errorTable = "error_monitor"
  var source = "source"
  var data = "data"
  var info = "info"

  var metricTable = "monitor"
  var data_source = "data_source"
  var file_name = "file_name"
  var metric_name = "metric_name"
  var dec_topic = "dec_topic"
  var src_topic = "src_topic"
  var monitor_time = "monitor_time"

  var datasource = "datasource"

  var fileName = "fileName"
  var host = "host"
  var metricName = "metricName"
  var tags = "tags"
  var timestamp = "timestamp"
  var time = "time"
  var monitorType = "type"
  var value = "value"
  var destination = "destination"

  var decTopic = "decTopic"
  var srcTopic = "srcTopic"
  var id = "id"
  var topic = "topic"

  val receivedFromMQ = "rcvdFromMQ"
  val sendToKafka = "sendToKafka"
  val receivedFromKafka = "rcvdFromKafka"
  val logFile = "LogFile"


  def load(document: Document): Monitor = {
    val monitor = new Monitor
    monitor.id = MD5Util.getMD5(UUID.randomUUID.toString.replaceAll("-", ""))
    monitor.dataSource = JunhaiLog.getString(document, datasource.toLowerCase())
    monitor.host = JunhaiLog.getString(document, host)
    monitor.metricName = JunhaiLog.getString(document, metricName)
    monitor.time = TimeUtil.time2SqlDate(JunhaiLog.getLong(document, Monitor.timestamp), TimeUtil.MILLISECOND)
    monitor.value = JunhaiLog.getInt(document, value)
    monitor.fileName = JunhaiLog.getString(document, fileName)
    monitor.monitorType = JunhaiLog.getString(document, monitorType)
    monitor.tags = JunhaiLog.getString(document, tags)
    monitor.topic = JunhaiLog.getString(document, topic)
    monitor
  }

  def unload(monitor: Monitor): Document = {
    val document = new Document()
    document.append(id, monitor.id)
      .append(data_source, monitor.dataSource)
      .append(host, monitor.host)
      .append(metric_name, monitor.metricName)
      .append(time, monitor.time)
      .append(value, monitor.value)
      .append(file_name, monitor.fileName)
      .append(monitorType, monitor.monitorType)
      .append(topic, monitor.topic)
      .append(tags, monitor.tags)
  }
}
