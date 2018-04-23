package com.ijunhai.storage.greenplum

import java.util
import java.util.Date

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.{JunhaiLog, Monitor}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document


object Save2GP {
  def monitor(GPSink: Broadcast[GreenPlumSink], source: String, map: util.HashMap[String, Int], monitorTable: String = Monitor.metricTable): Unit = {
    val time = TimeUtil.time2SqlDate(new Date().getTime, TimeUtil.MILLISECOND)
    val it = map.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val dest = if (entry.getKey == source) "" else entry.getKey
      val document = new Document()
        .append(Monitor.source, source)
        .append(Monitor.destination, dest)
        .append(Monitor.value, entry.getValue)
        .append(Monitor.monitor_time, time)
      GPSink.value.insert(document, monitorTable)
    }
  }


  def metricMonitor(GPSink: Broadcast[GreenPlumSink], dataSource: String,
                    serverName: String, metricName: String,
                    value: Int, typ: String,
                    timestamp: Long, fileName: String = "",
                    srcOrDec: String = "", tag: String = ""): Unit = {
    val srcOrDecData = if (srcOrDec.equals("")) dataSource else srcOrDec
    val document = new Document()
      .append(Monitor.datasource, dataSource)
      .append(Monitor.fileName, fileName)
      .append(Monitor.host, serverName)
      .append(Monitor.metricName, metricName)
      .append(Monitor.tags, tag)
      .append(Monitor.timestamp, timestamp)
      .append(Monitor.monitorType, typ)
      .append(Monitor.value, value)
      .append(Monitor.topic, srcOrDecData)
    GPSink.value.insert(Monitor.unload(Monitor.load(document)), Monitor.metricTable)
  }

  def collectMonitor(GPSink: Broadcast[GreenPlumSink], str: String): Unit = {
    val doc = Document.parse(str)
    doc.append(Monitor.topic, doc.getString(Monitor.datasource))
    GPSink.value.insert(Monitor.unload(Monitor.load(doc)), Monitor.metricTable)
  }

  def errorMonitor(GPSink: Broadcast[GreenPlumSink], source: String, info: String, data: String): Unit = {
    val document = new Document()
      .append(Monitor.source, source)
      .append(Monitor.info, info)
      .append(Monitor.data, data)
      .append(Monitor.time, TimeUtil.time2SqlDate(new Date().getTime, TimeUtil.MILLISECOND))
    GPSink.value.insert(document, Monitor.errorTable)
  }


  def saveDStream(ds: RDD[Document], bGreenPlumSink: Broadcast[GreenPlumSink], table: String): RDD[Document] = {
    if (!ds.isEmpty) {
      ds.mapPartitions(rdd => {
        rdd.map(document => {
          bGreenPlumSink.value.insert(document, table)
        })
      })
    } else {
      ds
    }
  }

  def SelectDStream(d: Document, bGreenPlumSink: Broadcast[GreenPlumSink], table: String): Int = {
    var result = 0
    if (!d.isEmpty) {
      result = bGreenPlumSink.value.Select(d, table)
    }
    result
  }

  def UpdateDStream(d: Document, bGreenPlumSink: Broadcast[GreenPlumSink], table: String): Int = {
    var result = 0
    if (!d.isEmpty) {
      result = bGreenPlumSink.value.Update(d, table)
    }
    result
  }
}
