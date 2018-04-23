package com.ijunhai.batch


import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.redis.RedisSink
import com.ijunhai.process.jhchannel.Log2GPProcess
import com.ijunhai.common.TimeUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by admin on 2018/1/26.
  */
object UpdateGP {
  val tableNameMap: Map[String, String] = Map("dalan" -> "dl_user", "chumeng" -> "cm_user")

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
//      .setMaster("local")
      .setAppName("UpdateGP")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sourcePath = args(0)//"hdfs://uhadoop-1neej2-master2/data/cm_user_20180125.txt" // hdfs://Ucluster/data/cm_user_20180125.txt
    val dataType = args(1)//"chumeng" //dalan chumeng
    val event = args(2)//"register" //register
    val redisSink = sc.broadcast(RedisSink.apply())
    val GPSink = sc.broadcast(GreenPlumSink.apply(if (dataType.equals(Log2GPProcess.DALAN)) Log2GPProcess.DALAN_DATABASE else Log2GPProcess.CHUMENG_DATABASE))
    val log = sc.textFile(sourcePath)

    log.mapPartitions(strings => strings.map(str => {
      val doc = Document.parse(str)
      val gameId = doc.get("jh_app_id")
      val deviceId = doc.get("device_id")
      val redisKey = "channel_" + dataType +"_"+ gameId + "_" + deviceId
      val redisTime = redisSink.value.hmget(redisKey, event + "_server_ts")
      val  redisTs: Int =
        if (redisTime!=null)
          redisTime.get(0).toInt
        else
          999999999
      if (redisTs==999999999) println(redisKey)

      val selectSql = "select register_time from " + tableNameMap.getOrElse(dataType,"") + " where game =  '" + gameId + "' and device_id = '" + deviceId + "'"
      val GPRS = GPSink.value.select(selectSql)
      while (GPRS.next()) {
        var newDevice = 0
        val timeString = GPRS.getString(1)
        val GPTs = TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", timeString, TimeUtil.SECOND).toInt

        if (redisTs < GPTs) {
          newDevice = 0
        } else {
          newDevice = 1 //新
        }
        val upDateSql = "update " + tableNameMap.getOrElse(dataType,"") + " set new_device=" + newDevice + " where game =  '" + gameId + "' and device_id = '" + deviceId + "' and register_time = '" + timeString + "'"
        GPSink.value.update(upDateSql)
      }
    })).count()
  }
}
