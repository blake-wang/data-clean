package com.dao

import java.util.Date

import com.ijunhai.common.logsystem.Monitor
import com.ijunhai.storage.greenplum.GreenPlumSink
import org.bson.Document

/**
  * Created by admin on 2018/1/30.
  */
object MonitorTest {
  def main(args: Array[String]): Unit = {
    val greenPlumSink = GreenPlumSink.apply("monitor")
    monitor(greenPlumSink, "test", "test", Monitor.receivedFromKafka, 999, Monitor.logFile,new Date().getTime)
  }

  def monitor(GPSink: GreenPlumSink, dataSource: String, serverName: String, metricName: String, value: Int,
              typ: String, timestamp: Long, fileName: String = "", srcOrDec: String = "", tag: String = ""): Unit = {
    val srcOrDecData = if (srcOrDec.equals("")) dataSource else srcOrDec
    val document = new Document()
      .append(Monitor.datasource, dataSource)
      .append(Monitor.fileName, fileName)
      .append(Monitor.host, serverName)
      .append(Monitor.metricName, metricName + "_" + srcOrDecData)
      .append(Monitor.tags, tag)
      .append(Monitor.timestamp, timestamp)
      .append(Monitor.monitorType, typ)
      .append(Monitor.value, value)
    GPSink.insert(Monitor.unload(Monitor.load(document)), Monitor.metricTable)
  }
}
