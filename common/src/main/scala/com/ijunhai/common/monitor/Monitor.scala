package com.ijunhai.common.monitor

import java.util

import com.ijunhai.common.monitor.MonitorType._
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * Created by admin on 2018/4/19.
  */
class Monitor(source: String, sparkContext: SparkContext) {
  val srcAcc = sparkContext.longAccumulator
  val destAcc = sparkContext.longAccumulator
  val otherAcc = sparkContext.longAccumulator
  val copyAcc = sparkContext.longAccumulator
  val testAcc = sparkContext.longAccumulator
  val filterAcc = sparkContext.longAccumulator

  def add(monitorType: MonitorType): Unit = {
    monitorType match {
      case OTHER =>
        otherAcc.add(1L)
      case DEST =>
        destAcc.add(1L)
      case COPY =>
        copyAcc.add(1L)
      case TEST =>
        testAcc.add(1L)
      case FILTER =>
        filterAcc.add(1L)
    }

    def doMonitor(dest: String,gpSink:Broadcast[GreenPlumSink]): Unit ={
      val map = new util.HashMap[String, Int]()
      map.put(source, srcAcc.value .toInt)
      map.put(dest + "Filter", filterAcc.value .toInt)
      source match {
        case "agentDBSrc" =>
          map.put(dest, destAcc.value .toInt)
          map put(dest + "Other", otherAcc.value .toInt)
          map.put(dest + "Copy", copyAcc.value.toInt)
          map.put(dest + "Test", testAcc.value.toInt)
        case "dalanDBSrc" =>
          map.put("agentDB", destAcc.value .toInt)
        case _ =>
          map.put(dest, destAcc.value.toInt)
      }
      Save2GP.monitor(gpSink, source, map)


    }




  }


}
