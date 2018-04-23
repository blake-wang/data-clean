package com.ijunhai.batch

import com.ijunhai.batch.UpdateGP.tableNameMap
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.redis.RedisSinkCluster
import com.ijunhai.process.jhchannel.Log2GPProcess
import com.ijunhai.common.TimeUtil
import org.bson.Document
import org.junit.Test

/**
  * Created by admin on 2018/1/26.
  */
@Test
object UpdateGPTest {
  @Test
  def updateGP {


    val str =""
    val dataType="dalan"
    val event="register"
    val redisSink = RedisSinkCluster.apply()
    val GPSink = GreenPlumSink.apply(if (dataType.equals(Log2GPProcess.DALAN)) Log2GPProcess.DALAN_DATABASE else Log2GPProcess.CHUMENG_DATABASE)

    val doc = Document.parse(str)
    val gameId = doc.get("jh_app_id")
    val deviceId = doc.get("device_id")
    val redisKey = "channel_" + dataType + gameId + "_" + deviceId
    val redisTs = redisSink.hmget(redisKey, event + "_server_ts").toString.toInt

    val selectSql = "select register_time from " + tableNameMap.get(dataType) + " where game =  '" + gameId + "' and device_id = '" + deviceId+"'"
    val GPRS = GPSink.select(selectSql)
    while (GPRS.next()) {
      var newDevice = 0
      val timeString = GPRS.getString(1)
      val GPTs = TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", timeString, TimeUtil.SECOND).toInt
      if (redisTs < GPTs) {
        newDevice = 0
      } else {
        newDevice = 1
      }
      val upDateSql = "update " + tableNameMap.get(dataType) + " set new_device='" + newDevice + "' where game =  '" + gameId + "' and device_id = " + deviceId + "and register_time = '" + timeString+"'"
      GPSink.update(upDateSql)
    }
  }
}
