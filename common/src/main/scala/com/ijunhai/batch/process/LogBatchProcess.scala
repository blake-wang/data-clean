package com.ijunhai.batch.process

import java.util
import java.util.Date

import com.ijunhai.common.CleanConstants.{REDISSINK, _}
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.{HDFSUtil, TimeUtil}
import com.ijunhai.process.agent.AgentProcess.getIOS128
import com.ijunhai.process.agent.KafkaLogProcess._
import com.ijunhai.process.agent.{AgentProcess, KafkaLogProcess}
import com.ijunhai.process.jhchannel.DB2GPProcess.{TABLE_ORDER_NAME, userTransform}
import com.ijunhai.process.saveFirstEvent
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
import com.ijunhai.storage.redis.Save2Redis.{LOGIN, ORDER, PARTITION_NUM}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConversions._


object LogBatchProcess {
  def commonBatch(rdd: RDD[String], service: String, destFilePath: String
                  , hbaseSink: Broadcast[HbaseSink], kafkaSink: Broadcast[KafkaSink], redisSink: Broadcast[RedisSink], boolean: Boolean
                 ): Unit = {
    val dataCleanConfig = HDFSUtil.readConfigFromHdfs(configPath)
    val bDataCleanConfig = rdd.sparkContext.broadcast(dataCleanConfig)
    val bBytes = rdd.sparkContext.broadcast(IPAddress.init(ipDatabasePath))
    if (!rdd.isEmpty()) {
      val resultRDD = rdd.map(log => {
        var result = true
        var temp = true
        try {
          val document = Document.parse(log)
          result = document.get(JunhaiLog.event) != null && document.getInteger(JunhaiLog.server_ts, 0) != 0 &&
            document.get(JunhaiLog.device) != null && document.get(JunhaiLog.game) != null &&
            document.get(JunhaiLog.is_test) != null && document.get(JunhaiLog.data_ver) != null
          if (service.equals("agent") && JunhaiLog.getSecondColumnString(document, JunhaiLog.agent, JunhaiLog.game_channel_id).equals("")
            && JunhaiLog.getSecondColumnString(document, JunhaiLog.game, JunhaiLog.game_id).toString.toInt >= 63) {
            result = false //game_id 大于63且 game_channel_id 为空的独代数据 过滤
          }

          if (result) {
            val eventRule = bDataCleanConfig.value.get(CONFIG_HEAD).asInstanceOf[Document].get(document.get(JunhaiLog.event)).asInstanceOf[Document]
            if (eventRule != null) {
              val firstColumns: util.Set[String] = eventRule.keySet()
              if (firstColumns != null) {
                firstColumns.foreach(firstColumn => {
                  result &&= document.get(firstColumn) != null
                  if (result) {
                    val firstColumnRule = eventRule.get(firstColumn).asInstanceOf[Document]
                    val secondColumns = firstColumnRule.keySet()
                    secondColumns.foreach(secondColumn => {
                      val value = document.get(firstColumn).asInstanceOf[Document].get(secondColumn)
                      result &&= value != null
                      if (result) {
                        val cleanType = firstColumnRule.getString(secondColumn)
                        try {
                          val tempResult = cleanType match {
                            case "0" =>
                              true
                            case "1" =>
                              value.asInstanceOf[String] != ""
                            case "2" =>
                              value.asInstanceOf[Int] > 0
                            case "3" =>
                              value.asInstanceOf[Double] > 0.0
                            case _ =>
                              value.asInstanceOf[String].matches(cleanType)
                          }
                          result &&= tempResult
                          if (!result && temp) {
                            Save2Kafka.saveErrorMetrics(document, new Date(), KafkaLogProcess, JunhaiLog.ERROR, s"Error value in $secondColumn column may be null.")
                            temp = false
                          }
                        } catch {
                          case e: Exception =>
                            e.printStackTrace()
                            Save2Kafka.saveErrorMetrics(document, new Date(), KafkaLogProcess, JunhaiLog.ERROR, s"Error value in $value may be not matched with $cleanType.")
                            result = false
                        }
                      }
                    })
                  } else {
                    if (temp) {
                      Save2Kafka.saveErrorMetrics(document, new Date(), KafkaLogProcess, JunhaiLog.ERROR, s"Error value in $firstColumn column may be null.")
                      temp = false
                    }
                  }
                })
              }
            }
          } else {
            Save2Kafka.saveErrorMetrics(document, new Date(), KafkaLogProcess, JunhaiLog.ERROR, "1.Error value in common column may be null.")
          }
          (document, result)
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            val doc = new Document().append(JunhaiLog.DATA, log)
            result = false
            Save2Kafka.saveErrorMetrics(doc, new Date(), KafkaLogProcess, JunhaiLog.ERROR, "2.Error value in common column may be null.")
            (doc, result)
        }
      }).mapPartitions(p => {
        val ipClass = new IP
        ipClass.load(bBytes.value)
        val temp = p.map { case (document, result) => {
          if (result) {
            try {
              val geo = IPAddress.getIPAddress(document.getString(JunhaiLog.client_ip), ipClass)
              for (key <- geo.keySet()) {
                document.put(key, geo.get(key))
              }
            } catch {
              case e: Exception =>
                document.put("province", "")
                document.put("country", "")
                document.put("city", "")
            }
          }
          (document, result)
        }
        }
        temp
      }).cache()

      //      Save2Kafka.saveDStreamAsync(resultRDD.filter(!_._2).map(line => {
      //        val log = line._1.get(JunhaiLog.LOG).asInstanceOf[Document]
      //        if (log != null) {
      //          line._1.remove(JunhaiLog.LOG)
      //          val data = line._1.get(JunhaiLog.DATA)
      //          if (data == null) {
      //            log.put(JunhaiLog.DATA, line._1)
      //          } else {
      //            log.put(JunhaiLog.DATA, data)
      //          }
      //          log.toJson()
      //        } else {
      //          line._1.toJson()
      //        }
      //      }), kafkaSink, service + ERROR_FLAG) //错误数据导入到错误kafka_topic中

      val afterCleanLog = resultRDD.filter(_._2).map(_._1)

      service match {
        case AGENT =>
          agentBatch(afterCleanLog, service, destFilePath, hbaseSink, kafkaSink, redisSink, boolean)
        case _ =>
          if (JunhaiLog.junhaiChannelsName.contains(service))
            jhChannelBatch(afterCleanLog, service, destFilePath, hbaseSink, kafkaSink, redisSink, boolean)
      }
      resultRDD.unpersist()
    }
  }

  def agentBatch(dStream: RDD[Document], service: String, destFilePath: String
                 , hbaseSink: Broadcast[HbaseSink], kafkaSink: Broadcast[KafkaSink], redisSink: Broadcast[RedisSink], boolean: Boolean): Unit = {
    val sparkContext = dStream.sparkContext
    val agcTuple = AgentProcess.getAgentGameChannel(hdfsCachePath)
    val bIOS128 = sparkContext.broadcast(getIOS128(hdfsCachePath))
    val bAcMap = sparkContext.broadcast(AgentProcess.getAgentChannel(hdfsCachePath))
    val bGgMap = sparkContext.broadcast(agcTuple._1)
    val bGcMap = sparkContext.broadcast(agcTuple._2)
    val bScMap = sparkContext.broadcast(AgentProcess.getSubChannel(hdfsCachePath))
    val bBscMap = sparkContext.broadcast(AgentProcess.getAllBySubChannel(hdfsCachePath))
    val bGcmMap = sparkContext.broadcast(AgentProcess.getAgentGame(hdfsCachePath))
    val log = dStream.map(bson => {
      val gameId = bson.get(JunhaiLog.game).asInstanceOf[Document].getString(JunhaiLog.game_id)
      val device = bson.get(JunhaiLog.device).asInstanceOf[Document]
      val agent = bson.get(JunhaiLog.agent).asInstanceOf[Document]
      val game = bson.get(JunhaiLog.game).asInstanceOf[Document]
      val channelId = JunhaiLog.getString(agent, JunhaiLog.channel_id)
      val gameChannelId = agent.getString(JunhaiLog.game_channel_id)
      val result = bson.get(JunhaiLog.agent) != null && gameId != "" && gameId != null && JunhaiLog.isNumber(gameId) &&
        !(bson.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id) == "" && bson.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.game_channel_id) == "")
      if (!result) {
        Save2Kafka.saveErrorMetrics(bson, new Date(), KafkaLogProcess, "BATCHERROR", "Error value in " + JunhaiLog.serviceAgent + " column may be null.")
      } else {
        if (channelId.equals("")) { //需要找回channel_id
          val value = bGgMap.value.getOrElse(gameId + gameChannelId, error)
          agent.put(JunhaiLog.channel_id, value)
        } else if (gameChannelId.equals("") || gameChannelId.equals("0")) {
          //   独代日志 gameid>=63，gameChannelId为空的已经在前面全部过滤了
          if (!bScMap.value.contains(channelId)) {
            //http://game.data.ijunhai.com/Gamedata/api/getAgentSubChannel
            //此时order表中的channel_id就是channel_id
            val value = bGcMap.value.getOrElse(gameId + channelId, error) //拿到game_channel_id
            val subPf = bAcMap.value.getOrElse(channelId, error)
            agent.put(JunhaiLog.game_channel_id, value)
            bson.put(JunhaiLog.os_type, subPf)
          } else {
            //有子渠道subchannel的关联
            //http://game.data.ijunhai.com/Gamedata/api/getAgentOldGameChannel
            val agentChannelId = bScMap.value.getOrElse(channelId, error)
            agent.put(JunhaiLog.channel_id, agentChannelId)
            val value = bBscMap.value.getOrElse(channelId, error) //此时的channel_id中方的是sub_channel_id的值根据sub_channel拿game_channel_id
            //出现subchannelId匹配不到的情况，则直接使用agentchannelId去匹配得到gamechannelid
            if (value.equals(error)) {
              val agentValue = bGcMap.value.getOrElse(gameId + agentChannelId, error)
              agent.put(JunhaiLog.game_channel_id, agentValue)
            } else {
              agent.put(JunhaiLog.game_channel_id, value)
            }
            val subPf = bAcMap.value.getOrElse(agentChannelId, error)
            device.put(JunhaiLog.os_type, subPf)
          }
        }
        val companyId = bGcmMap.value.getOrDefault(gameId, error) //游戏公司关联
        game.put(JunhaiLog.company_id, companyId)
      }
      (bson, result)
    }).filter { case (bson, result) => { //剔除大蓝等自有渠道的数据
      !JunhaiLog.junhaiChannelsId.contains(bson.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id))
    }
    }
    val afterCleanLog = log.filter(_._2).map(_._1)
    //    val withNewUserFlag = save2RedisBatch(afterCleanLog, service, redisSink)
    val accum = dStream.sparkContext.longAccumulator("longAccum")

    val finalRDD = afterCleanLog.map(log => {
      if (boolean) {
        KafkaLogProcess.documentFlat("", saveFirstEvent.save(log, hbaseSink, redisSink))
      } else {
        KafkaLogProcess.documentFlat("", saveFirstEvent.saveBatch(log, hbaseSink))
      }
    })

    if (destFilePath != "") {
//      finalRDD.cache()
//      finalRDD.filter((line: Document) => {
//        bIOS128.value.contains(line.getString("agent-game_channel_id")) //gameId小于128的ios独代
//      }).map(_.toJson()).saveAsTextFile(destFilePath + "Other")

      finalRDD.filter((line: Document) => {
        !bIOS128.value.contains(line.getString("agent-game_channel_id"))
      }).map(_.toJson()).saveAsTextFile(destFilePath)
//      finalRDD.unpersist()
    } else {
      finalRDD.count()
    }


  }

  def jhChannelBatch(rdd: RDD[Document], service: String, destFilePath: String
                     , hbaseSink: Broadcast[HbaseSink], kafkaSink: Broadcast[KafkaSink], redisSink: Broadcast[RedisSink], boolean: Boolean): Unit = {
    val gameChannel = AgentProcess.getAgent2GameChannel(hdfsCachePath, service)
    val bGameChannel = rdd.sparkContext.broadcast(gameChannel)
    val (rmb, usd) = AgentProcess.getRate(hdfsCachePath)
    val bRmb = rdd.sparkContext.broadcast(rmb)
    val bUsd = rdd.sparkContext.broadcast(usd)
    val gcmMap = AgentProcess.getAgentGame(hdfsCachePath)
    val bGcmMap = rdd.sparkContext.broadcast(gcmMap)

    val log = channelProcess(rdd, service)
    val cleanLog = transformBatch(log, bGameChannel, bRmb, bUsd, bGcmMap, service)
    val changePhase = cleanLog.map { case (doc, flag) => {
      if (flag) {
        val appId = JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.app_id)
        val gameId = JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.game_id)
        val document = new Document()

        for (key <- doc.keySet()) {
          if (key != JunhaiLog.game)
            document.put(key, doc.get(key))
        }
        val newGame = new Document()
        newGame.put(JunhaiLog.game_id, appId)
        newGame.put(JunhaiLog.app_id, gameId)
        newGame.put(JunhaiLog.game_ver, JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.game_ver))
        newGame.put(JunhaiLog.game_name, JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.game_name))
        newGame.put(JunhaiLog.company_id, JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.company_id))
        document.put(JunhaiLog.game, newGame)


        (document, flag)
      } else
        (doc, flag)
    }
    }

    val afterCleanLog = changePhase.filter(_._2).map(_._1)
      //首次事件
      .map(log =>
      if (boolean) {
        KafkaLogProcess.documentFlat("", saveFirstEvent.save(log, hbaseSink, redisSink))
      } else {
        KafkaLogProcess.documentFlat("", saveFirstEvent.saveBatch(log, hbaseSink))
      }

    )

    if (destFilePath != "") {
      afterCleanLog.saveAsTextFile(destFilePath)
    } else {
      afterCleanLog.count()
    }

    //    withNewUserFlag.map(_._1).saveAsTextFile(destFilePath)

  }


  def save2RedisBatch(rdd: RDD[(Document)], service: String, redisSinkCluster: Broadcast[RedisSink]): RDD[Document] = {
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
              if (JunhaiLog.agentName.contains(service)) {
                agent.getString(JunhaiLog.channel_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
              } else if (JunhaiLog.channelsWithOutDalan.contains(service)) {
                service + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, JunhaiLog.game_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
              } else if (service == JunhaiLog.serviceSDK) {
                game.getString(JunhaiLog.game_name) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.junhai_sdk, JunhaiLog.app_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
              } else {
                service + log.get(JunhaiLog.device).asInstanceOf[Document].getString(JunhaiLog.device_id)
              }
            //user/order
            case _ =>
              if (JunhaiLog.agentName.contains(service)) { //
                //agent_channelId_gameId_userId
                agent.getString(JunhaiLog.channel_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, JunhaiLog.game_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.user, JunhaiLog.user_id)
              } else if (JunhaiLog.channelsWithOutDalan.contains(service)) {
                //channel_dalan_gameId_userId
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
            val historyTime: String = values.get(0)
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
            //            if ((historyTime == null || historyTime.length() < 10 || historyTime.toInt > serverTs.toInt)
            //              && log.getString(JunhaiLog.is_test).equals("regular")
            //              && (service.equals("agent") || (service.equals("dalan") && !event.equals("register")) || (service.equals("haiwai") && !event.equals("register")))
            //              && (event != ORDER || (event == ORDER && JunhaiLog.getSecondColumnString(log, JunhaiLog.order, JunhaiLog.order_status).equals("success") && JunhaiLog.getSecondColumnString(log, JunhaiLog.order, JunhaiLog.order_step).equals("paid_success")))) {
            //              redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "")
            //            }

            if ((historyTime == null || historyTime.length() < 10 || historyTime.toInt > serverTs.toInt)
              && log.getString(JunhaiLog.is_test).equals("regular")
              && (service.equals("agent") || (service.equals("dalan") && !event.equals("register")) || (service.equals("haiwai") && !event.equals("register")))
              && (event != ORDER || (event == ORDER && JunhaiLog.getString(log, "pay_status").equals("1")))) {
              redisSinkCluster.value.hset(key, event + "_" + JunhaiLog.server_ts, serverTs + "") //历史数据不插入
            }


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


  val error = "error"
  val FORMAT_STR = "yyyy-MM-dd HH:mm:ss"
  val HWGameMap = Map("hw_csgj" -> "100000000", "hw_hwdzz" -> "100000001", "hw_djcz" -> "100000002",
    "hw_csgjhwb" -> "100000003", "hw_lzdl" -> "100000004", "hw_csgjhwbfoyo" -> "100000005",
    "hw_lzdlhwb" -> "100000006", "hw_tw_lzdl" -> "100000007", "hw_tw_lzdl3" -> "100000008",
    "hw_tw_lzdl4" -> "100000009", "hw_jt_wyfx" -> "100000010", "hw_web_pay" -> "100000011",
    "hw_tw_lzdl5" -> "100000012", "hw_tw_android_yjqy" -> "100000013", "hw_tw_ios_yjqy" -> "100000014",
    "hw_sgqj" -> "100000015", "hw_tw_ios_sgqj" -> "100000016", "hw_wzsy" -> "100000017",
    "hw_hw_android_HWwzsy" -> "100000018", "hw_hw_tw_android_HW_jyqy" -> "100000019", "hw_csssj" -> "100000020",
    "hw_XJZZ" -> "100000021", "hw_xjzz_standalone" -> "100000022", "hw_hw_tw_google_xjzz" -> "100000023",
    "hw_hw_tw_standalone_xjzz" -> "100000024", "hw_hw_tw_standalone_sgqj_test" -> "100000025",
    "hw_hw_tw_google" -> "100000026", "hw_hw_tw_google_sgqj" -> "100000027", "hw_hw_tw_standalone_sgqj" -> "100000028",
    "hw_hw_tw_ios_xjzz" -> "100000029")

  /**
    * 关联上独代的数据channel_id ,game_channel_id,game_id一共两个接口，其中一个belong的是IOS海外一些没有走独代的数据，另外一个接口是走独代的数据关联接口
    *
    * @param rdd
    * @param bGameChannel
    * @return
    */
  def transformBatch(rdd: RDD[(Document, Boolean)]
                     , bGameChannel: Broadcast[Map[String, (String, String, String)]]
                     , bRmb: Broadcast[Map[String, Double]]
                     , bUsd: Broadcast[Map[String, Double]]
                     , bGcmMap: Broadcast[Map[String, String]]
                     , service: String): RDD[(Document, Boolean)] = {

    service match {
      case "haiwai" =>
        rdd.map { case (doc, flag) => {
          if (flag) {
            val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
            val agent = JunhaiLog.getDocument(doc, JunhaiLog.agent)
            //开始关联
            var columnValue = JunhaiLog.getString(game, JunhaiLog.game_id) //默认的独代的channel_id字段
            columnValue = HWGameMap.getOrElse(columnValue, columnValue)
            val values = bGameChannel.value.getOrElse(columnValue, (error, error, error))
            val gameChannelId = values._1
            val channelId = values._2
            val gameId = values._3

            game.put("app_id", gameId) //这条语句必须要在上面
            //            game.put(JunhaiLog.game_id,gameId)
            game.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId, error))

            agent.put(JunhaiLog.channel_id, channelId)
            agent.put(JunhaiLog.game_channel_id, gameChannelId)

            val event = JunhaiLog.getString(doc, JunhaiLog.event) //添加汇率转换
            if (event == JunhaiLog.order) {
              val order = JunhaiLog.getDocument(doc, JunhaiLog.order)
              val currencyAmount = JunhaiLog.getDouble(order, JunhaiLog.currency_amount)
              val currencyType = JunhaiLog.getString(order, JunhaiLog.currency_type)
              val rmb = bRmb.value.getOrElse(currencyType, 1.0) * currencyAmount
              val usd = bUsd.value.getOrElse(currencyType, 1.0) * currencyAmount
              order.append(JunhaiLog.cny_amount, rmb)
              order.append(JunhaiLog.usd_amount, usd)
            }

            doc.put(JunhaiLog.agent, agent)
            (doc, flag)
          } else {
            (doc, flag)
          }
        }
        }
      case _ =>
        println("dalan")
        rdd.map { case (doc, flag) => {
          if (flag) {
            val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
            val agent = JunhaiLog.getDocument(doc, JunhaiLog.agent)
            val columnValue = JunhaiLog.getString(game, JunhaiLog.game_id) //默认的独代的channel_id字段
            val values = bGameChannel.value.getOrElse(columnValue, (error, error, error))
            val gameChannelId = values._1
            val channelId = values._2
            val gameId = values._3

            game.put("app_id", gameId)
            //            game.replace(JunhaiLog.game_id,gameId)
            //            newGame.put(JunhaiLog.game_name,JunhaiLog.getString(game,JunhaiLog.game_name))
            //            newGame.put(JunhaiLog.game_ver,JunhaiLog.getString(game,JunhaiLog.game_ver))
            //            game.put(JunhaiLog.game_id,gameId)
            //            println("columnValue"+columnValue)
            game.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId, error))
            agent.put(JunhaiLog.channel_id, channelId)
            agent.put(JunhaiLog.game_channel_id, gameChannelId)
            doc.put(JunhaiLog.agent, agent)


            (doc, flag)
          } else {
            (doc, flag)
          }
        }
        }
    }
  }

  def cmDBProcess(rdd: RDD[String], system: String, dbType: String
                  , gpSink: Broadcast[GreenPlumSink], redisSink: Broadcast[RedisSink]): Unit = {
    val ORDER = "order"
    val USER = "user"
    if (!rdd.isEmpty()) {
      val document = rdd.map { msg => {
        try {
          val doc = Document.parse(msg)
          (doc, true)
        } catch {
          case e: Exception =>
            System.err.println(new Date() + " Error: json parse error!")
            (new Document(AgentProcess.LOG, msg), false)
        }
      }
      }
      if (dbType == USER) {
        userTransform(document.map(_._1), gpSink, system, redisSink).count()
      } else {
        orderTransformBatch(document.map(_._1), gpSink, TABLE_ORDER_NAME).count()
      }

      //      val orderData = filterDocuments.filter { case (document, tableName, type_) => {
      //        //表两条日志
      //        // 1、pay_status=0 and status=1 下单
      //        // 2、pay_status=1 and status=2 支付收到钱。
      //        tableName == ORDER && ((JunhaiLog.getInt(document, "pay_status") == 0 && JunhaiLog.getInt(document, "status") == 1)
      //          || (JunhaiLog.getInt(document, "pay_status") == 1 && JunhaiLog.getInt(document, "status") == 2))
      //      }}
      //
      //      val userData = filterDocuments.filter { case (document, tableName, type_) => {
      //        tableName == USER && type_ == "INSERT"
      //      }}
      //
      //      //存GP
      //      val orderDocument = orderTransform(orderData.map(_._1), bGreenPlumSink, TABLE_ORDER_NAME)
      //      val userDocument = userTransform(userData.map(_._1), bGreenPlumSink, topic, redisSinkCluster)
      //      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), Log2GPProcess.getClass.getName, JunhaiLog.INFO, "DB2GP: after save to GP ", "Num", "orderDocument:" + orderDocument.count() + " userDocument:" + userDocument.count())

      //      //存kafka
      //      Save2Kafka.saveDStreamAsync(orderDocument.map(_.toJson()), kafkaSink, topic + "Order", "com.ijunhai.kafka.mq2kafka.DB2GP")
      //      Save2Kafka.saveDStreamAsync(userDocument.map(_.toJson()), kafkaSink, topic + "User", "com.ijunhai.kafka.mq2kafka.DB2GP")
      //      if (offset != null) {
      //        println("update offset")
      //        topics.toArray().foreach(topic => {
      //          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
      //        })
      //      }
    }
  }

  def orderTransformBatch(rDD: RDD[Document]
                          , bGreenPlumSink: Broadcast[GreenPlumSink]
                          , table: String): RDD[Document] = {
    val documents = rDD.map(document => {
      CmOrder.unload(CmOrder.load(document))
    })
    Save2GP.saveDStream(documents, bGreenPlumSink, table)
  }

}
