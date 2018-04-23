package com.ijunhai.storage.redis

import java.sql.Date
import java.util

import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.TimeUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

/**
  * Created by Admin on 2017-08-14.
  */
object Save2Redis {
  val FORMAT_STR = "yyyy-MM-dd"
  val ORDER = "order"
  val LOGIN = "login"
  val REGISTER = "register"
  val PARTITION_NUM = 10

  /**
    * 将数据存储到redis中
    *
    * @param rdd     数据
    * @param service 服务 独代，SDK，渠道
    *                数据源(实时以及批处理)
    *                独代日志 (login)
    *                大蓝/海外 login日志(login)，user(register)/order(order)数据库
    */
  def saveDStream(rdd: RDD[(Document)], service: String, redisSinkCluster: Broadcast[RedisSink]): RDD[Document] = {
    if (!rdd.isEmpty()) {
      rdd.repartition(PARTITION_NUM).mapPartitions(p => {
        //        lazy val redisConn = RedisClient.getInstatnce.getJedis

        val result = p.map(log => {
          val event = log.getString(JunhaiLog.event)
          val agent = log.get(JunhaiLog.agent).asInstanceOf[Document]
          val game = log.get(JunhaiLog.game).asInstanceOf[Document]
          val headInfo = event match {
            //激活
            case JunhaiLog.eventActive | JunhaiLog.eventCrash =>
              //              agent.getString(JunhaiLog.channel_id) + "_" +log.get(JunhaiLog.device).asInstanceOf[Document].getString(JunhaiLog.device_id)
              if (JunhaiLog.agentName.contains(service)) { //dalan 和 agent
                agent.getString(JunhaiLog.channel_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
              } else if (JunhaiLog.channelsWithOutDalan.contains(service)) { //不包括大蓝
                service + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, JunhaiLog.game_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
              } else if (service == JunhaiLog.serviceSDK) {
                game.getString(JunhaiLog.game_name) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.junhai_sdk, JunhaiLog.app_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
              } else {
                service + log.get(JunhaiLog.device).asInstanceOf[Document].getString(JunhaiLog.device_id)
              }
            //user/order
            case _ =>
              if (JunhaiLog.agentName.contains(service)) { //agent和大蓝
                //agent_channelId_gameId_userId
                if (log.get(JunhaiLog.user) == null || agent == null)
                  println(log)
                agent.getString(JunhaiLog.channel_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, JunhaiLog.game_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.user, JunhaiLog.user_id)
              } else if (JunhaiLog.channelsWithOutDalan.contains(service)) {
                //channel_haiwai_gameId_userId
                service + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, JunhaiLog.game_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.user, JunhaiLog.user_id)
              } else if (service == JunhaiLog.serviceSDK) {
                //sdk_gameId_userId
                game.getString(JunhaiLog.game_name) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.junhai_sdk, JunhaiLog.app_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.user, JunhaiLog.user_id)
              } else {
                service + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, JunhaiLog.game_id) + JunhaiLog.getSecondColumnString(log, JunhaiLog.user, JunhaiLog.user_id)
              }
          }
          if (headInfo != "") {
            //需要保存首次操作时间的事件
            val key = if (JunhaiLog.channelsWithOutDalan.contains(service)) {
              "channel" + "_" + headInfo
            } else {
              "agent" + "_" + headInfo
            }
            val serverTs = JunhaiLog.getTimestamp(log, JunhaiLog.server_ts)

            val values = redisSinkCluster.value.hmget(key, event + "_" + JunhaiLog.server_ts, LOGIN + "_" + JunhaiLog.server_ts, ORDER + "_" + JunhaiLog.server_ts)
            val historyTime = values.get(0)
            val loginHistoryTime = values.get(1)
            val orderHistoryTime = values.get(2)
            val serverDate = TimeUtil.time2DateString(FORMAT_STR, serverTs.toInt, TimeUtil.SECOND)
            val serverDateHour = TimeUtil.time2DateString("HH", serverTs.toInt, TimeUtil.SECOND)
            val serverDateMinute = TimeUtil.time2DateString("mm", serverTs.toInt, TimeUtil.SECOND)
            //插入redis需满足的条件：
            //1.redis该值为空，redis时间大于数据时间，满足其一
            //2.is_test字段为regular
            //3.独代login数据（order在另一个类），大蓝/海外首次登录时间作为login的数据（不保留表user（即register）数据，保留order数据（即order）），
            //4.event不为order，或者event为order时order-order_status为success且order-order_step为paid_success(批处理只需判断order-order_status为success，另外打包)
            if ((historyTime == null || historyTime.length() < 10 || historyTime.toInt > serverTs.toInt)
              && log.getString(JunhaiLog.is_test).equals("regular")
              && (service.equals("agent") || (service.equals("dalan") && !event.equals("register")) || (service.equals("haiwai") && !event.equals("register")))
              && (event != ORDER || (event == ORDER && JunhaiLog.getSecondColumnString(log, JunhaiLog.order, JunhaiLog.order_status).equals("success") && JunhaiLog.getSecondColumnString(log, JunhaiLog.order, JunhaiLog.order_step).equals("paid_success")))) {
              redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "")
            }

            //            if ((historyTime == null || historyTime.length() < 10 || historyTime.toInt > serverTs.toInt)
            //              && log.getString(JunhaiLog.is_test).equals("regular")
            //              && (service.equals("agent") || (service.equals("dalan") && !event.equals("register")) || (service.equals("haiwai") && !event.equals("register")))
            //              && (event != ORDER || (event == ORDER && JunhaiLog.getSecondColumnString(log, JunhaiLog.order, JunhaiLog.order_status).equals("success") ))) {
            //              redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "")
            //            }


            //首付
            val firstOrderDate = if (orderHistoryTime == null || (event.equals(ORDER) && orderHistoryTime.toInt > serverTs)) {
              if (event == ORDER)
                serverDate
              else
                "" //login数据无首付的情况
            } else {
              TimeUtil.time2DateString(FORMAT_STR, orderHistoryTime.toInt, TimeUtil.SECOND)
            }
            //首登(order数据无登录的情况把支付时间当成注册时间)
            val firstLoginDate = if (loginHistoryTime == null || (event == LOGIN && loginHistoryTime.toInt > serverTs.toInt)) {
              //              if (event == LOGIN)
              serverDate
              //              else
              //                "" //order数据无登录的情况
            } else {
              TimeUtil.time2DateString(FORMAT_STR, loginHistoryTime.toInt, TimeUtil.SECOND)
            }

            log.put(JunhaiLog.first_order_date, firstOrderDate) //首次充值时间
            log.put(JunhaiLog.server_date + "_day", serverDate) //当前服务时间
            log.put(JunhaiLog.server_date + "_hour", serverDateHour) //当前服务时间
            log.put(JunhaiLog.server_date + "_minute", serverDateMinute) //当前服务时间
            log.put(JunhaiLog.reg_date, firstLoginDate) //首次登陆时间
          }
          log
        })
        //        RedisClient.returnResource(redisConn)
        result
      })
    } else {
      rdd
    }
  }

  //独代order
  def saveDatabaseOrder(rdd: RDD[(Document)], service: String, redisSinkCluster: Broadcast[RedisSink]): RDD[Document] = {
    val result = if (!rdd.isEmpty()) {
      rdd.repartition(PARTITION_NUM).mapPartitions(p => {
        val result = p.map(log => {
          val event = ORDER
          val headInfo = if (service == JunhaiLog.serviceAgent) {
            //key:agent_channelId_gameId_userId
            log.get(JunhaiLog.channel_id) + "_" + log.get(JunhaiLog.game_id) + "_" + log.get(JunhaiLog.user_id)
          } else {
            log.getInteger(JunhaiLog.user_id)
          }
          if (headInfo != "") {
            //需要保存首次操作时间的事件
            val key = service + "_" + headInfo
            var serverTs = 0L
            try {
              serverTs = log.getLong("update_time") / 1000
              log.put("update_time", serverTs.toInt)
            } catch {
              case ex: Exception =>
                ex.printStackTrace()
            }
            val values = redisSinkCluster.value.hmget(key, event + "_" + JunhaiLog.server_ts, "login" + "_" + JunhaiLog.server_ts)
            val historyTime = values.get(0)
            val loginHistoryTime = values.get(1)
            val serverDate = TimeUtil.time2DateString(FORMAT_STR, serverTs.toInt, TimeUtil.SECOND)
            val serverDateHour = TimeUtil.time2DateString("HH", serverTs.toInt, TimeUtil.SECOND)
            val serverDateMinute = TimeUtil.time2DateString("mm", serverTs.toInt, TimeUtil.SECOND)

            //首冲
            val firstEventTime = if ((historyTime == null || historyTime.toInt > serverTs.toInt)
              && log.get("pay_status").toString.equals("1") && log.get("status").toString.equals("2")) {
              //pay_status和status判断完才插入
              redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "")
              serverDate
            } else if (historyTime == null || historyTime.toInt > serverTs.toInt) {
              serverDate
            } else {
              TimeUtil.time2DateString(FORMAT_STR, historyTime.toInt, TimeUtil.SECOND)
            }

            //首登
            val firstLoginDate = if (loginHistoryTime == null)
              serverDate //首付无登录情况
            else
              TimeUtil.time2DateString(FORMAT_STR, loginHistoryTime.toInt, TimeUtil.SECOND)

            log.put(JunhaiLog.first_order_date, firstEventTime) //首次充值时间
            log.put(JunhaiLog.server_date + "_day", serverDate) //当前服务时间
            log.put(JunhaiLog.server_date + "_hour", serverDateHour) //当前服务时间
            log.put(JunhaiLog.server_date + "_minute", serverDateMinute) //当前服务时间
            log.put(JunhaiLog.reg_date, firstLoginDate) //首次登陆时间
          }
          log
        })
        result
      })
    } else {
      rdd
    }
    result
  }

  /**
    * 依赖redis 的expire的滑动时间窗口去重
    *
    * @param rdd
    * @return
    */
  def distinctByWindow[T](rdd: RDD[(T, String)], redisSinkCluster: Broadcast[RedisSink]): RDD[(T, String)] = {
    if (!rdd.isEmpty()) {
      rdd.repartition(PARTITION_NUM).mapPartitions(p => {
        //        lazy val redisConn = RedisClient.getInstatnce.getJedis
        val result = p.map { case (doc, key) => {
          val flag = redisSinkCluster.value.exists(key)
          if (!flag) {
            //如果不存在
            redisSinkCluster.value.setex(key, 60 * 5, "")
            //            redisConn.expire(key,60*5)//5分钟
          }
          (doc, key, flag)
        }
        }
        result
      }).filter(!_._3).map(line => (line._1, line._2))
    } else {
      rdd
    }
  }

  /**
    * 判断任务是否正在运行
    *
    * @param key    streaming应用标识
    * @param second 当前streaming任务的间隔时间
    * @return
    */
  def isRunning(key: String, uuid: String, second: Int, redisSinkCluster: Broadcast[RedisSink]): Boolean = {
    //    val redisConn = RedisClient.getInstatnce.getJedis
    val value = redisSinkCluster.value.get(key)
    val result = if (value == null) {
      redisSinkCluster.value.setex(key, second * 2, uuid)
      uuid
    } else {
      value
    }
    //    if(redisSinkCluster!=null)
    //      redisConn.close()
    result != uuid
  }

  //独代支付批处理
  def saveDatabaseOrderBatch(rdd: RDD[(Document)], service: String, redisSinkCluster: Broadcast[RedisSink]): RDD[Document] = {
    val result = if (!rdd.isEmpty()) {
      rdd.repartition(PARTITION_NUM).mapPartitions(p => {
        //        lazy val redisConn = RedisClient.getInstatnce.getJedis || historyTime.toInt > serverTs.toInt
        val result = p.map(log => {
          val event = ORDER
          val headInfo = if (service == JunhaiLog.serviceAgent) {
            log.get(JunhaiLog.channel_id) + "_" + log.get(JunhaiLog.game_id) + "_" + log.get(JunhaiLog.user_id)
          } else {
            log.getInteger(JunhaiLog.user_id)
          }
          if (headInfo != "") {
            val key = service + "_" + headInfo
            var serverTs = 0L
            try {
              serverTs = TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", log.getString("update_time"), TimeUtil.SECOND)
              log.put("update_time", serverTs.toInt)
            } catch {
              case ex: Exception =>
                ex.printStackTrace()
            }
            val values = redisSinkCluster.value.hmget(key, event + "_" + JunhaiLog.server_ts, "login" + "_" + JunhaiLog.server_ts)
            val historyTime = values.get(0)
            val loginHistoryTime = values.get(1)
            val serverDate = TimeUtil.time2DateString(FORMAT_STR, serverTs.toInt, TimeUtil.SECOND)
            val serverDateHour = TimeUtil.time2DateString("HH", serverTs.toInt, TimeUtil.SECOND)
            val serverDateMinute = TimeUtil.time2DateString("mm", serverTs.toInt, TimeUtil.SECOND)
            val firstEventTime = if ((historyTime == null || historyTime.toInt > serverTs.toInt)
              && log.get("pay_status").toString.equals("1")) {
              //历史数据从数据库中导，只需要判断，pay_status=1
              //之前不存在数据表中，代表是第一次接入 log.getString()会报错
              redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "")
              serverDate
            } else if (historyTime == null || historyTime.toInt > serverTs.toInt) {
              serverDate
            } else {
              TimeUtil.time2DateString(FORMAT_STR, historyTime.toInt, TimeUtil.SECOND)
            }
            val firstLoginDate = if (loginHistoryTime == null)
              serverDate
            else
              TimeUtil.time2DateString(FORMAT_STR, loginHistoryTime.toInt, TimeUtil.SECOND)
            log.put(JunhaiLog.first_order_date, firstEventTime) //首次充值时间
            log.put(JunhaiLog.server_date + "_day", serverDate) //当前服务时间
            log.put(JunhaiLog.server_date + "_hour", serverDateHour) //当前服务时间
            log.put(JunhaiLog.server_date + "_minute", serverDateMinute) //当前服务时间
            log.put(JunhaiLog.reg_date, firstLoginDate) //首次登陆时间
          }
          log
        })
        result
      })
    } else {
      rdd
    }
    result
  }

  /**
    * key value形式的数据保存
    *
    * @param rDD
    */
  def saveKeyValueAlone(rDD: RDD[(String, String)], persistTime: Int = 0, redisSinkCluster: Broadcast[RedisSink]): RDD[(String, String)] = {
    require(persistTime >= 0)
    if (!rDD.isEmpty()) {
      rDD.mapPartitions(p => {
        //        lazy val redisConn = RedisClientAlone.getInstatnce.getJedis
        val result = p.map(log => {
          if (persistTime == 0) {
            redisSinkCluster.value.set(log._1, log._2)
          } else {
            redisSinkCluster.value.setex(log._1, persistTime, log._2)
          }
          log
        })
        result
      })
    } else {
      rDD
    }
  }

  /**
    * key value形式的数据保存
    *
    * @param rDD
    */
  def saveKeyValueAlone(rDD: RDD[(String, String)], redisSink: Broadcast[RedisSinkAlone], persistTime: Int): RDD[(String, String)] = {
    require(persistTime >= 0)
    if (!rDD.isEmpty()) {
      rDD.mapPartitions(p => {
        val result = p.map(log => {
          if (persistTime == 0) {
            redisSink.value.set(log._1, log._2)
          } else {
            redisSink.value.setex(log._1, persistTime, log._2)
          }
          log
        })
        result
      })
    } else {
      rDD
    }
  }


  def isAgentNewDevice(doc: Document, service: String, redisSinkCluster: Broadcast[RedisSink],
                       eventType: String): Document = {

    val key = service + "_" + JunhaiLog.getString(doc, "channel_id") + "_" +
      JunhaiLog.getString(doc, "game_id") + "_" + JunhaiLog.getString(doc, "device_id")

    val redisTs: util.List[String] = redisSinkCluster.value.hmget(key, eventType + "_" + JunhaiLog.server_ts)
    val serverTs: Long = JunhaiLog.getLong(doc,JunhaiLog.server_ts)
    val values: Int = if (redisTs.get(0) != null) {
      if (serverTs > redisTs.get(0).toLong) {
        0 //不是新设备
      } else {
        redisSinkCluster.value.hset(key, eventType + "_" + JunhaiLog.server_ts, serverTs + "")
        1
      }
    } else {
      redisSinkCluster.value.hset(key, eventType + "_" + JunhaiLog.server_ts, serverTs + "")
      1 //新设备
    }
    doc.append("new_device", values)
  }


  /**
    * 是否是新设备信息判断(user表使用)
    *
    * @param rdd
    * @param service
    * @param deviceKey
    * @param timeKey
    * @return
    */
  def isNewDevice(rdd: RDD[Document], service: String, gameKey: String, deviceKey: String, timeKey: String, redisSinkCluster: Broadcast[RedisSink], eventType: String): RDD[(Document, Int)] = {

    rdd.repartition(PARTITION_NUM).mapPartitions(p => {
      //      lazy val redisConn = RedisClient.getInstatnce.getJedis
      val result = p.map(log => {
        val event = eventType //区分user和active数据
        val headInfo = service + "_" + JunhaiLog.getString(log, gameKey) + "_" + JunhaiLog.getString(log, deviceKey)
        //需要保存首次操作时间的事件
        val key = if (JunhaiLog.junhaiChannelsName.contains(service)) {
          "channel" + "_" + headInfo //channel_chumeng_gameId_deviceId   register_server_ts
        } else {
          service + "_" + headInfo
        }
        val redisTs = redisSinkCluster.value.hmget(key, event + "_" + JunhaiLog.server_ts)

        val serverTs: Long = log.get(timeKey).asInstanceOf[Date].getTime / 1000

        val values = if (redisTs.get(0) != null) { //user数据新设备判断，只要为空都为新设备否则都为老设备 ；同时更新最早时间
          if (serverTs > redisTs.get(0).toLong) {
            0 //不是新设备
          } else {
            redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "") //问题后面的数据先到会有两个1（cm游戏不准）
            1
          }
        } else {
          redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "")
          1 //新设备
        }
        //            log.put(saveKey,values ) //首次登陆时间
        (log, values)
      })
      //        RedisClient.returnResource(redisConn)
      result
    })
  }

  /**
    * 是否是新设备信息判断
    *
    * @param rdd
    * @param service
    * @param deviceKey
    * @param timeKey
    * @return
    */
  def isNewDeviceActive(rdd: RDD[Document], service: String, gameKey: String, deviceKey: String, timeKey: String, redisSinkCluster: Broadcast[RedisSink]): RDD[(Document, Int)] = {

    rdd.repartition(PARTITION_NUM).mapPartitions(p => {
      //      lazy val redisConn = RedisClient.getInstatnce.getJedis
      val result = p.map(log => {
        val event = JunhaiLog.eventActive

        val headInfo = service + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, gameKey) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, deviceKey)
        //需要保存首次操作时间的事件
        val key = if (JunhaiLog.junhaiChannelsName.contains(service)) {
          "channel" + "_" + headInfo
        } else {
          service + "_" + headInfo
        }
        val serverTs = JunhaiLog.getString(log, timeKey)
        val values = if (redisSinkCluster.value.exists(key)) {
          0 //不是新设备
        } else {
          redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "")
          1
        }
        //            log.put(saveKey,values ) //首次登陆时间
        (log, values)
      })
      //        RedisClient.returnResource(redisConn)
      result
    })
  }
}
