package com.ijunhai.process.agent

import java.util
import java.util.Date
import com.ijunhai.common.CleanConstants._

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.{DalanUser, JunhaiLog}
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.bson.Document

import scala.collection.JavaConversions._

object KafkaLogProcess {
  val ERROR_FLAG = "Error"
  val RIGHT_FLAG = "Std"
  val SDK = "sdk"
  val CHANNEL = "channel"
  val AGENT = "agent"

  val CONFIG_HEAD = "config"
  val DATA = "data"
  val USER_PER_LINE = 1
  val RETAINED_PACKAGE_ID = "packageID"
  val RETAINED_SERVICE = "service"
  val FORMAT_STR = "yyyy-MM-dd"
  val error = "error"
  val ACTIVE = "active"

  def activeProcess(ds: DStream[String], kafkaSink: Broadcast[KafkaSink], service: String
                    , dataCleanConfig: Document
                    , redisSinkCluster: Broadcast[RedisSink]
                    , nameServer: String
                    , gpSink: Broadcast[GreenPlumSink]): Unit = {
    val sc = ds.context.sparkContext
    val agentGame = AgentProcess.getAgentGameChannel(hdfsCachePath, 1)
    val bGgMap = sc.broadcast(agentGame._1) //(doc.getString(game_channel_id), doc.getString(channel_id) + "|" + doc.getString(game_id))
    val bGcMap = sc.broadcast(agentGame._2)
    //    http://game.data.ijunhai.com/Gamedata/api/getAgentGameChannel
    val bGcmMap = sc.broadcast(AgentProcess.getAgentGame(hdfsCachePath))
    val bScMap: Broadcast[Map[String, String]] = sc.broadcast(AgentProcess.getSubChannel(hdfsCachePath))
    //    http://game.data.ijunhai.com/Gamedata/api/getAgentSubChannel
    ds.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(log => {
          val info: ((Document, Int, String), Boolean) =
            try {
              (JunhaiLog.BsonParser(log, "ACTIVE_LOG["), true)
            } catch {
              case ex: Exception =>
                ex.printStackTrace()
                kafkaSink.value.send("monitor", "AgentActive: json parse error:" + log)
                ((new Document(), 0, ""), false)
            }
          if (info._2) {
            val srcDoc = info._1._1
            val serverTs = info._1._2
            val doc = srcDoc.get("app_data").asInstanceOf[Document]
            val extraData = srcDoc.get("extra_data").asInstanceOf[Document]
            for (k <- extraData.keySet()) {
              doc.append(k, JunhaiLog.getString(extraData, k))
            }
            val gameChannelId = try {
              JunhaiLog.getInt(doc, "game_channel_id")
            } catch {
              case ex: Exception =>
                ex.printStackTrace()
                kafkaSink.value.send("monitor", "AgentActive: game_channel_id error:" + log)
                0
            }
            //关联逻辑
            //game_id 小于63, game_channel_id有的是正常的channel_id, 有的为belong_channel_id,
            //game_id 大于等于63,根据game_channel_id 获得game_id和game_channel_id (因为game_id有可能不准)
            if (gameChannelId != 0) {
              val agentGameId = JunhaiLog.getInt(doc, "agent_game_id")

              val values =
                if (agentGameId < 63) {
                  val channelId =
                    if (bScMap.value.contains(gameChannelId.toString)) {
                      doc.remove(JunhaiLog.game_channel_id)
                      doc.append(JunhaiLog.game_channel_id, bGcMap.value.getOrElse(agentGameId.toString + gameChannelId.toString, error))
                      //game_channel_id 为 belong_channel_id
                      bScMap.value.getOrElse(gameChannelId.toString, error)
                    } else {
                      //game_channel_id 为 channel_id
                      gameChannelId.toString
                    }
                  doc.remove(JunhaiLog.game_channel_id)
                  doc.append(JunhaiLog.game_channel_id, bGcMap.value.getOrElse(agentGameId.toString + channelId, error))
                  (channelId, agentGameId.toString)
                } else {
                  val value = bGgMap.value.getOrElse(gameChannelId.toString, error).split("\\|")
                  (value(0), value(1))
                }

              doc.append(JunhaiLog.channel_id, values._1).append(JunhaiLog.game_id, values._2)
                .append(JunhaiLog.company_id, bGcmMap.value.getOrDefault(values._2, error)).append(JunhaiLog.event, "active")
                .append(JunhaiLog.server_ts, serverTs).append("ip", info._1._3)
                .append(JunhaiLog.server_date + "_day", TimeUtil.time2DateString(FORMAT_STR, serverTs, TimeUtil.SECOND))
                .append(JunhaiLog.server_date + "_hour", TimeUtil.time2DateString("HH", serverTs, TimeUtil.SECOND))
                .append(JunhaiLog.server_date + "_minute", TimeUtil.time2DateString("mm", serverTs, TimeUtil.SECOND))
              doc.remove("agent_game_id")

              kafkaSink.value.send("active",
                Save2Redis.isAgentNewDevice(doc, "agent", redisSinkCluster, "active").toJson())
            }
          }
        })
      }
    })
  }

  def process(ds: DStream[String], kafkaSink: Broadcast[KafkaSink], service: String
              , dataCleanConfig: Document
              , ipDatabasePath: String
              , redisSinkCluster: Broadcast[RedisSink]
              , nameServer: String
              , gpSink: Broadcast[GreenPlumSink]): Unit = {

    val bDataCleanConfig: Broadcast[Document] = ds.context.sparkContext.broadcast(dataCleanConfig)
    val bBytes = ds.context.sparkContext.broadcast(IPAddress.init(ipDatabasePath))
    ds.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val time = new Date().getTime
        //        Save2GP.metricMonitor(gpSink, service, nameServer, Monitor.receivedFromKafka, rdd.count().toInt, Monitor.logFile, time)

        val resultRDD: RDD[(Document, Boolean)] = rdd.map(log => {
          //通用数据清洗
          var result = true
          var jsonParse = true
          var temp = true
          var flag = true
          val document: (Document, Boolean) =
            if (service.equals("agent")) {
              if (!log.contains("ACTIVE_LOG")) { //独代登录日志用老格式
                val info: (Document, Int, String) = try {
                  JunhaiLog.agentOldLog2Bson(log)
                } catch {
                  case ex: Exception =>
                    ex.printStackTrace()
                    val doc = new Document().append(JunhaiLog.DATA, log)
                    jsonParse = false
                    Save2GP.errorMonitor(gpSink, service, "json parse error", log)
                    Save2Kafka.saveErrorMetrics(doc, new Date(), KafkaLogProcess, JunhaiLog.ERROR, "json parse error")
                    (new Document().append(LOG, log), 0, error)
                }
                val tmpDoc = info._1
                if (tmpDoc.getOrDefault(JunhaiLog.game_channel_id, "").equals("")
                  && tmpDoc.getOrDefault(JunhaiLog.game_id, "0").toString.toInt >= 63) {
                  flag = false
                }
                val userDoc = new Document().append(JunhaiLog.user_id, tmpDoc.getOrDefault(JunhaiLog.user_id, "")).append(JunhaiLog.user_name, "").append(JunhaiLog.gender, "").append(JunhaiLog.birth, "").append(JunhaiLog.age, "")
                val gameDoc = new Document().append(JunhaiLog.game_id, tmpDoc.getOrDefault(JunhaiLog.game_id, "")).append(JunhaiLog.game_name, "").append(JunhaiLog.game_ver, "")
                val agentDoc = new Document().append(JunhaiLog.channel_id, tmpDoc.getOrDefault(JunhaiLog.channel_id, "")).append(JunhaiLog.game_channel_id, tmpDoc.getOrDefault(JunhaiLog.game_channel_id, "")).append(JunhaiLog.access_token, tmpDoc.getOrDefault(JunhaiLog.access_token, ""))
                val deviceDoc = new Document().append(JunhaiLog.screen_height, "").append(JunhaiLog.screen_width, "").append(JunhaiLog.device_id, "").append(JunhaiLog.ios_idfa, "").append(JunhaiLog.android_imei, "").append(JunhaiLog.android_adv_id, "").append(JunhaiLog.android_id, "").append(JunhaiLog.device_name, "").append(JunhaiLog.os_ver, "").append(JunhaiLog.sdk_ver, "").append(JunhaiLog.package_name, "").append(JunhaiLog.os_type, "").append(JunhaiLog.net_type, "").append(JunhaiLog.user_agent, "")
                val document = new Document().append(JunhaiLog.user, userDoc).append(JunhaiLog.game, gameDoc).append(JunhaiLog.agent, agentDoc).append(JunhaiLog.device, deviceDoc)
                  .append(JunhaiLog.event, "login").append(JunhaiLog.is_test, "regular").append(JunhaiLog.data_ver, "1.0").append(JunhaiLog.client_time_zone, "").append(JunhaiLog.server_time_zone, "+08:00").append(JunhaiLog.client_ts, 0).append(JunhaiLog.server_ts, info._2).append(JunhaiLog.client_ip, info._3)
                (document, true)
              } else { //独代激活数据
                //                val info: Document = try {
                //                  JunhaiLog.agentActiveLog(log)
                //                } catch {
                //                  case ex: Exception =>
                //                    ex.printStackTrace()
                //                    val doc = new Document().append(JunhaiLog.DATA, log)
                //                    jsonParse = false
                //                    Save2GP.errorMonitor(gpSink, service, "json parse error", log)
                //                    Save2Kafka.saveErrorMetrics(doc, new Date(), KafkaLogProcess, JunhaiLog.ERROR, "json parse error")
                //                    new Document().append(LOG, log)
                //                }
                //                //"agent_game_id":"121","game_channel_id":"101647","action_type":"start","time":"1520058629"
                //                val appData = info.get("app_data").asInstanceOf[Document]
                //                val extraData = info.get("extra_data").asInstanceOf[Document]
                //                extraData.append("game_id",appData.getString("agent_game_id")).append("game_channel_id",appData.getString("game_channel_id"))
                //                  .append("action_type",appData.getString("action_type")).append("time",appData.getString("time")).append("event","active")
                //                //    "screen_size":"960|540","device_id":"00000000-3be3-e5e3-dfc7-96034d1a4a02","device_name":"RwoZI nPuU617",
                //                //    "imsi":"865166027042699","imei":"865166027042699","system_version":"5.1.1","sdk_version":"2.1",
                //                //    "package_name":"com.junhai.shj2.dalan","application_name":"山海经2","application_version":181},

                kafkaSink.value.send("agentActive", log)
                (new Document(), false)
              }
            } else {
              //新格式
              try {
                JunhaiLog.log2bson(log)
              } catch {
                case ex: Exception =>
                  ex.printStackTrace()
                  val doc = new Document().append(JunhaiLog.DATA, log)
                  jsonParse = false
                  Save2GP.errorMonitor(gpSink, service, "json parse error", log)
                  Save2Kafka.saveErrorMetrics(doc, new Date(), KafkaLogProcess, JunhaiLog.ERROR, "2.Error value in common column may be null.")
                  (new Document().append(LOG, log), false)
              }
            }
          val doc = document._1
          result = doc.get(JunhaiLog.event) != null &&
            JunhaiLog.getInt(doc, JunhaiLog.server_ts) != -1 &&
            doc.get(JunhaiLog.server_time_zone) != null &&
            doc.get(JunhaiLog.device) != null &&
            doc.get(JunhaiLog.game) != null &&
            doc.get(JunhaiLog.is_test) != null &&
            doc.get(JunhaiLog.client_ts) != null &&
            doc.get(JunhaiLog.client_time_zone) != null &&
            doc.get(JunhaiLog.data_ver) != null && jsonParse && document._2 && flag

          if (result) {
            val eventRule = bDataCleanConfig.value.get(CONFIG_HEAD).asInstanceOf[Document]
              .get(doc.get(JunhaiLog.event)).asInstanceOf[Document]
            if (eventRule != null) {
              val firstColumns = eventRule.keySet()
              if (firstColumns != null) {
                firstColumns.foreach(firstColumn => {
                  result &&= doc.get(firstColumn) != null
                  if (result) {
                    val firstColumnRule = eventRule.get(firstColumn).asInstanceOf[Document]
                    val secondColumns = firstColumnRule.keySet()
                    secondColumns.foreach(secondColumn => {
                      var value = doc.get(firstColumn).asInstanceOf[Document].get(secondColumn)
                      result &&= value != null
                      if (result) {
                        val cleanType = firstColumnRule.getString(secondColumn)
                        val tempResult = cleanType match {
                          case "0" =>
                            true
                          case "1" =>
                            value = JunhaiLog.getSecondColumnString(doc, firstColumn, secondColumn).asInstanceOf[AnyRef]
                            JunhaiLog.getSecondColumnString(doc, firstColumn, secondColumn) != ""
                          case "2" =>
                            value = JunhaiLog.getSecondColumnInteger(doc, firstColumn, secondColumn).asInstanceOf[AnyRef]
                            JunhaiLog.getSecondColumnInteger(doc, firstColumn, secondColumn) >= 0
                          case "3" =>
                            value = JunhaiLog.getSecondColumnDouble(doc, firstColumn, secondColumn).asInstanceOf[AnyRef]
                            JunhaiLog.getSecondColumnDouble(doc, firstColumn, secondColumn) > 0.0
                          case _ =>
                            value = JunhaiLog.getSecondColumnString(doc, firstColumn, secondColumn).asInstanceOf[AnyRef]
                            value.asInstanceOf[String].matches(cleanType)
                        }
                        result &&= tempResult
                        if (!result && temp) {
                          Save2GP.errorMonitor(gpSink, service, s"$secondColumn may be null", doc.toJson())
                          //                          Save2Kafka.saveErrorMetrics(doc, new Date(), KafkaLogProcess, JunhaiLog.ERROR, s"Error value in $secondColumn column may be null.")
                          temp = false
                        }
                      }
                    })
                  } else {
                    if (temp) {
                      //                      Save2GP.errorMonitor(gpSink, service, s"$firstColumn may be null", doc.toJson())
                      //Save2Kafka.saveErrorMetrics(doc, new Date(), KafkaLogProcess, JunhaiLog.ERROR, s"Error value in $firstColumn column may be null.")
                      temp = false
                    }
                  }
                })
              }
            }
          }
          (doc, result)
        }).mapPartitions(p => {
          val ipClass: IP = new IP
          ipClass.load(bBytes.value)
          val temp = p.map { case (document, result) => {
            if (result) {
              try {
                val geo: util.HashMap[String, String] = IPAddress.getIPAddress(document.getString(JunhaiLog.client_ip), ipClass)
                for (key <- geo.keySet()) {
                  var value: String = geo.get(key)
                  if (value.contains("\"")) {
                    value = value.replaceAll("\"", "")
                  }
                  document.put(key, value)
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

        Save2Kafka.save(resultRDD.filter(!_._2)
          .map(line => {
            line._1.toString
          })
          //          .map(line => {
          //          val log = line._1.get(JunhaiLog.LOG).asInstanceOf[Document]
          //          if (log != null) {
          //            line._1.remove(JunhaiLog.LOG)
          //            val data = line._1.get(JunhaiLog.DATA)
          //            if (data == null) {
          //              log.put(JunhaiLog.DATA, line._1)
          //            } else {
          //              log.put(JunhaiLog.DATA, data)
          //            }
          //            log.toJson()
          //          } else {
          //            line._1.toJson()
          //          }
          //        })
          , kafkaSink, gpSink, service + ERROR_FLAG, nameServer) //过滤或者错误数据导入到错误kafka_topic中

        val afterCleanLog = resultRDD.filter(_._2).map(_._1)

        service match {
          case SDK =>
            SDKProcess(afterCleanLog, kafkaSink, service, redisSinkCluster, gpSink, nameServer)
          case CHANNEL =>
            otherChannelProcess(afterCleanLog, kafkaSink, service, redisSinkCluster, gpSink, nameServer)
          case AGENT => //agent
            agentProcess(afterCleanLog, kafkaSink, service, redisSinkCluster, gpSink, nameServer)
          case _ =>
            if (JunhaiLog.junhaiChannelsName.contains(service)) //dalan haiwai
              junhaiChannelProcess(afterCleanLog, kafkaSink, service, redisSinkCluster, gpSink, nameServer)
        }
        resultRDD.unpersist()
      }

    })
  }


  /**
    * 独代数据处理
    */
  def agentProcess(dstream: RDD[Document], kafkaSink: Broadcast[KafkaSink],
                   service: String,
                   redisSinkCluster: Broadcast[RedisSink],
                   gpSink: Broadcast[GreenPlumSink],
                   nameServer: String): Unit = {

    val sparkContext = dstream.sparkContext
    val agcTuple = AgentProcess.getAgentGameChannel(hdfsCachePath)
    val bAcMap = sparkContext.broadcast(AgentProcess.getAgentChannel(hdfsCachePath))
    val bGgMap = sparkContext.broadcast(agcTuple._1)
    val bGcMap = sparkContext.broadcast(agcTuple._2)
    val bScMap = sparkContext.broadcast(AgentProcess.getSubChannel(hdfsCachePath))
    val bBscMap = sparkContext.broadcast(AgentProcess.getAllBySubChannel(hdfsCachePath))
    val bGcmMap = sparkContext.broadcast(AgentProcess.getAgentGame(hdfsCachePath))
    val bFilter = sparkContext.broadcast(AgentProcess.getFilterRule(hdfsCachePath))


    val log = dstream.map(bson => {
      val game = bson.get(JunhaiLog.game).asInstanceOf[Document]
      val gameId = game.getString(JunhaiLog.game_id)
      val device = bson.get(JunhaiLog.device).asInstanceOf[Document]
      var result = bson.get(JunhaiLog.agent) != null && //这个判断好像可以删除
        gameId != "" && !(bson.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id) == ""
        && bson.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.game_channel_id) == "")
      val agent = bson.get(JunhaiLog.agent).asInstanceOf[Document]
      val channelId = agent.getString(JunhaiLog.channel_id)
      val gameChannelId = agent.getString(JunhaiLog.game_channel_id)

      //      if (bson.getString(JunhaiLog.event) == "login"
      //        && (agent.getString(JunhaiLog.access_token) == null || agent.getString(JunhaiLog.access_token) == "")) {
      //                System.err.println(new Date()+" ERROR: Error value in " + JunhaiLog.access_token + " may be null.")
      //        result = false
      //      }

      if (!result) {
        Save2Kafka.saveErrorMetrics(bson, new Date(), KafkaLogProcess, JunhaiLog.ERROR, "Error value in " + JunhaiLog.serviceAgent + " column may be null.")
        //        System.err.println(new Date()+" ERROR: Error value in " + JunhaiLog.serviceAgent + " may be null.")
      } else {
        //通过清洗
        if (channelId.equals("")) {
          //需要找回channel_id
          val value = bGgMap.value.getOrElse(gameId + gameChannelId, error)
          agent.put(JunhaiLog.channel_id, value)
        } else if (gameChannelId.equals("") || gameChannelId.equals("0")) {
          //   独代日志 gameid>=63，gameChannelId为空的已经在前面全部过滤了
          //          if (gameId.toInt >= 63) {
          //            val value: String = bGgMap.value.getOrElse(gameId + channelId, error)
          //            val subPf = bAcMap.value.getOrElse(value, error)
          //            agent.put(JunhaiLog.channel_id, value)
          //            agent.put(JunhaiLog.game_channel_id, channelId) //将原来的字段名字改回来
          //            bson.put(JunhaiLog.os_type, subPf)
          //          } else {
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
            if (value.equals(error)) { //出现subchannelId匹配不到的情况，则直接使用agentchannelId去匹配得到gamechannelid
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
    }).filter { case (bson, result) => { //剔除haiwai的数据
      !JunhaiLog.haiwaiChannelId.contains(bson.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id))
    }
    }
    commonProcess(log, kafkaSink, service, redisSinkCluster, gpSink, nameServer)
  }

  /**
    * 渠道数据处理
    */
  def channelProcess(dstream: RDD[Document], service: String): RDD[(Document, Boolean)] = {
    dstream.map(bson => {
      val result = bson.get(JunhaiLog.channel_platform) != null &&
        JunhaiLog.getSecondColumnString(bson, JunhaiLog.channel_platform, JunhaiLog.ad_id) != "" &&
        JunhaiLog.getSecondColumnString(bson, JunhaiLog.game, JunhaiLog.game_id) != ""
      if (!result) {
        //        Save2Kafka.saveMetrics(bson,new Date(),DataProcess,JunhaiLog.ERROR,"Error value in "+JunhaiLog.serviceChannel+" column may be null.")
        //        System.err.println(new Date()+" ERROR: Error value in " + JunhaiLog.serviceChannel + " may be null.")
      }
      (bson, result)
    })
  }

  /**
    * 非自有渠道通用处理
    */
  def otherChannelProcess(dstream: RDD[Document], kafkaSink: Broadcast[KafkaSink], service: String,
                          redisSinkCluster: Broadcast[RedisSink], gpSink: Broadcast[GreenPlumSink],
                          nameServer: String
                         ): Unit = {
    val log = channelProcess(dstream, service)
    commonProcess(log, kafkaSink, service, redisSinkCluster, gpSink, nameServer)
  }

  def junhaiChannelProcess(rdd: RDD[Document], kafkaSink: Broadcast[KafkaSink],
                           service: String,
                           redisSinkCluster: Broadcast[RedisSink],
                           gpSink: Broadcast[GreenPlumSink],
                           nameServer: String): Unit = {
    val gameChannel = AgentProcess.getAgent2GameChannel(hdfsCachePath, service)
    val bGameChannel = rdd.sparkContext.broadcast(gameChannel)
    val (rmb, usd) = AgentProcess.getRate(hdfsCachePath)
    val bRmb = rdd.sparkContext.broadcast(rmb)
    val bUsd = rdd.sparkContext.broadcast(usd)
    val gcmMap = AgentProcess.getAgentGame(hdfsCachePath)
    val bGcmMap = rdd.sparkContext.broadcast(gcmMap)
    val bIOS128 = rdd.sparkContext.broadcast(AgentProcess.getIOS128(hdfsCachePath))

    val log = channelProcess(rdd, service)

    val cleanLog = ChannelTransform.transform(log, bGameChannel, bRmb, bUsd, bGcmMap, service, bIOS128)
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

        if (newGame.getString(JunhaiLog.app_id).equals("100000011")) {
          //海外的特殊belong_game_id，走网页支付，直接过滤掉
          (document, false)
        } else {
          (document, flag)
        }
      } else
        (doc, flag)
    }
    }

    commonProcess(changePhase, kafkaSink, service, redisSinkCluster, gpSink, nameServer) //自有渠道走自有渠道的登陆判断
  }

  /**
    * SDK数据处理
    */
  def SDKProcess(rdd: RDD[Document], kafkaSink: Broadcast[KafkaSink], service: String,
                 redisSinkCluster: Broadcast[RedisSink],
                 gpSink: Broadcast[GreenPlumSink],
                 nameServer: String): Unit = {
    val log = rdd.map(bson => {
      val result = bson.get(JunhaiLog.junhai_sdk) != null &&
        bson.get(JunhaiLog.junhai_sdk).asInstanceOf[Document].getString(JunhaiLog.app_id) != ""
      if (!result) {
        Save2Kafka.saveErrorMetrics(bson, new Date(), KafkaLogProcess, JunhaiLog.ERROR, "Error value in " + JunhaiLog.serviceSDK + " column may be null.")
        //        System.err.println(new Date()+" ERROR: Error value in " + JunhaiLog.serviceSDK + " may be null.")
      }
      (bson, result)
    })
    commonProcess(log, kafkaSink, service, redisSinkCluster, gpSink, nameServer)
  }


  def commonProcess(log: RDD[(Document, Boolean)],
                    kafkaSink: Broadcast[KafkaSink],
                    service: String,
                    redisSinkCluster: Broadcast[RedisSink],
                    gpSink: Broadcast[GreenPlumSink],
                    nameServer: String): Unit = {
    val iOS128 = AgentProcess.getIOS128(hdfsCachePath)
    val bIOS128 = log.sparkContext.broadcast(iOS128)
    val afterCleanLog = log.filter(_._2).map(_._1)
    val withNewUserFlag = Save2Redis.saveDStream(afterCleanLog, service, redisSinkCluster).map(line => {
      val result: (Document, String) = if (line.getString(JunhaiLog.event) == ACTIVE) {
        (documentFlat("", line, service).append("user-user_id", ""), JunhaiLog.getSecondColumnString(line, JunhaiLog.agent, JunhaiLog.game_channel_id)) //kylin中需要这个字段来对其数据，实际上激活是没有用户ID这个字段的
      } else {
        (documentFlat("", line, service), JunhaiLog.getSecondColumnString(line, JunhaiLog.agent, JunhaiLog.game_channel_id))
      }
      (result._1.toJson(), result._2)
    })
    Save2Kafka.save(log.filter(!_._2).map(line => {
      val log = line._1.get(JunhaiLog.LOG).asInstanceOf[Document]
      if (log != null) {
        line._1.remove(JunhaiLog.LOG)
        val data = line._1.get(JunhaiLog.DATA)
        if (data == null) {
          log.put(JunhaiLog.DATA, line._1)
        } else {
          log.put(JunhaiLog.DATA, data)
        }
        log.toJson()
      } else {
        line._1.toJson()
      }
    }), kafkaSink, gpSink, service + ERROR_FLAG, nameServer)
    if (service == JunhaiLog.serviceAgent) {
      Save2Kafka.save(withNewUserFlag.filter(line => {
        bIOS128.value.contains(line._2)
      }).map(_._1), kafkaSink, gpSink, service + RIGHT_FLAG + AgentProcess.OTHER_FLAG, nameServer)
      Save2Kafka.save(withNewUserFlag.filter(line => {
        !bIOS128.value.contains(line._2)
      }).map(_._1), kafkaSink, gpSink, service + RIGHT_FLAG, nameServer)
    } else if (service == JunhaiLog.serviceDalan) { //大蓝数据合并到独代
      Save2Kafka.save(withNewUserFlag.map(_._1), kafkaSink, gpSink, JunhaiLog.serviceAgent + RIGHT_FLAG, nameServer)
    } else {
      Save2Kafka.save(withNewUserFlag.map(_._1), kafkaSink, gpSink, service + RIGHT_FLAG, nameServer)
    }
  }

  def retainWrite(data: RDD[Document], service: String): Unit = {
    service match {
      case SDK =>
        data.map(document => {
          val serverTime = document.getInteger(JunhaiLog.server_ts).toLong
          val app_id = document.get(JunhaiLog.junhai_sdk).asInstanceOf[Document].getString(JunhaiLog.app_id)
          //          val game_channel_id=document.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.game_channel_id)
          //          val ad_id=document.get(JunhaiLog.channel_platform).asInstanceOf[Document].getString(JunhaiLog.ad_id)
          //          val game_id=document.get(JunhaiLog.game).asInstanceOf[Document].getString(JunhaiLog.game_id)
          //          val channel_id=document.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id)
          val os_type = document.get(JunhaiLog.device).asInstanceOf[Document].getString(JunhaiLog.os_type)
          val event = document.getString(JunhaiLog.event)
          val map = new util.HashMap[String, String]()
          map.put(JunhaiLog.app_id, app_id)
          map.put(JunhaiLog.os_type, os_type)
          //          map.put(JunhaiLog.game_channel_id,game_channel_id)
          //          map.put(JunhaiLog.ad_id,ad_id)
          //          map.put(JunhaiLog.game_id,game_id)
          //          map.put(JunhaiLog.channel_id,channel_id)
          //          RetainedWriter.getInstatnce.write( service + "-" + event, serverTime, map, USER_PER_LINE)
        })
      case CHANNEL =>
        data.map(document => {
          val serverTime = document.getInteger(JunhaiLog.server_ts).toLong
          //          val app_id=document.get(JunhaiLog.junhai_sdk).asInstanceOf[Document].getString(JunhaiLog.app_id)
          //          val game_channel_id=document.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.game_channel_id)
          val ad_id = document.get(JunhaiLog.channel_platform).asInstanceOf[Document].getString(JunhaiLog.ad_id)
          val game_id = document.get(JunhaiLog.game).asInstanceOf[Document].getString(JunhaiLog.game_id)
          //          val channel_id=document.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id)
          val os_type = document.get(JunhaiLog.device).asInstanceOf[Document].getString(JunhaiLog.os_type)
          val event = document.getString(JunhaiLog.event)
          val map = new util.HashMap[String, String]()
          //          map.put(JunhaiLog.app_id,app_id)
          map.put(JunhaiLog.os_type, os_type)
          //          map.put(JunhaiLog.game_channel_id,game_channel_id)
          map.put(JunhaiLog.ad_id, ad_id)
          map.put(JunhaiLog.game_id, game_id)
          //          map.put(JunhaiLog.channel_id,channel_id)
          //          RetainedWriter.getInstatnce.write(service + "-" + event, serverTime, map, USER_PER_LINE)
        })
      case AGENT =>
        data.map(document => {
          val serverTime = document.getInteger(JunhaiLog.server_ts).toLong
          //          val app_id = document.get(JunhaiLog.junhai_sdk).asInstanceOf[Document].getString(JunhaiLog.app_id)
          val game_channel_id = document.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.game_channel_id)
          //          val ad_id=document.get(JunhaiLog.channel_platform).asInstanceOf[Document].getString(JunhaiLog.ad_id)
          val game_id = document.get(JunhaiLog.game).asInstanceOf[Document].getString(JunhaiLog.game_id)
          val channel_id = document.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id)
          val os_type = document.get(JunhaiLog.device).asInstanceOf[Document].getString(JunhaiLog.os_type)
          val event = document.getString(JunhaiLog.event)
          val map = new util.HashMap[String, String]()
          //          map.put(JunhaiLog.app_id, app_id)
          map.put(JunhaiLog.os_type, os_type)
          map.put(JunhaiLog.game_channel_id, game_channel_id)
          //          map.put(JunhaiLog.ad_id,ad_id)
          map.put(JunhaiLog.game_id, game_id)
          map.put(JunhaiLog.channel_id, channel_id)
          //          RetainedWriter.getInstatnce.write(service + "-" + event, serverTime, map, USER_PER_LINE)
        })
    }
  }

  //  def documentFlat(parentKey:String,document:Document):Document={
  //    val doc=new Document()
  //    document.keySet().foreach(key=>{
  //      document.get(key) match {
  //        case value:Document=>
  //          doc.putAll(documentFlat(parentKey+key,value))
  //        case value:_=>
  //          doc.put(parentKey+key,value)
  //      }
  //    })
  //    doc
  //  }

  def documentFlat(parentKey: String, document: Document, source: String): Document = {
    var afterFlat = new Document()
    if (source.equals("dalanDBSrc")) { //dalan订单字段整合成独代的样子
      afterFlat = KafkaLogProcess.dalanDocFlat(parentKey, document)
    } else {
      afterFlat = KafkaLogProcess.documentFlat(parentKey, document)
    }
    afterFlat
  }

  def documentFlat(parentKey: String, document: Document): Document = {
    val connectSimple = "-"
    val doc = new Document()
    for (key <- document.keySet()) {
      val value = document.get(key)
      value match {
        case value: Document =>
          val temp = documentFlat(parentKey + connectSimple + key, value)
          for (key1 <- temp.keySet().toArray) {
            if (parentKey != "")
              doc.put(key1.asInstanceOf[String], temp.get(key1.asInstanceOf[String]))
            else
              doc.put(key1.asInstanceOf[String].substring(1), temp.get(key1.asInstanceOf[String]))
          }
        case value: Array[Document] =>
          val arrValue = value.map(doc => {
            documentFlat(parentKey + connectSimple + key, doc)
          })
          doc.put(key.asInstanceOf[String], arrValue)
        case _ =>
          if (parentKey != "")
            doc.put(parentKey + connectSimple + key, value)
          else
            doc.put(key.asInstanceOf[String], value)
      }
    }
    doc
  }

  def dalanDocFlat(parentKey: String, document: Document): Document = {
    val doc = new Document()
    for (key <- document.keySet()) {
      val value = document.get(key)
      value match {
        case value: Document =>
          val temp = dalanDocFlat(key, value)
          for (key1 <- temp.keySet().toArray) {
            if (parentKey != "")
              doc.put(key1.asInstanceOf[String], temp.get(key1.asInstanceOf[String]))
            else
              doc.put(key1.asInstanceOf[String], temp.get(key1.asInstanceOf[String]))
          }
        case value: Array[Document] =>
          val arrValue = value.map(doc => {
            dalanDocFlat(key, doc)
          })
          doc.put(key.asInstanceOf[String], arrValue)
        case _ =>
          if (parentKey != "")
            doc.put(key, value)
          else
            doc.put(key.asInstanceOf[String], value)
      }
    }
    doc
  }

  val LOG = "log"

  /**
    * 订单关联处理
    */
  def orderJoinTest(ds: DStream[String]
                    , kafkaSink: Broadcast[KafkaSink]
                    , column: String
                    , topic: String
                    , hdfsCachePath: String
                    , redisSinkCluster: Broadcast[RedisSink]): Unit = {

    println("------------" + System.currentTimeMillis() + "------------")

    ds.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val rows = "rows"
        val table = "table"
        val data = "data"
        val order_sn = "order_sn"
        val database = "database"
        val actionType = "type"
        val agent_order = "agent_order"
        val agent_order_copy = "agent_order_copy"
        val game_id = "game_id"
        val game_channel_id = "game_channel_id"
        val channel_id = "channel_id"
        val sub_pf = "sub_pf"

        val agcTuple = AgentProcess.getAgentGameChannel(hdfsCachePath)
        val acMap = AgentProcess.getAgentChannel(hdfsCachePath)
        val scMap = AgentProcess.getSubChannel(hdfsCachePath)
        val bscMap = AgentProcess.getAllBySubChannel(hdfsCachePath)
        val ggMap = agcTuple._1
        val gcMap = agcTuple._2

        val bAcMap = rdd.sparkContext.broadcast(acMap)
        val bGgMap = rdd.sparkContext.broadcast(ggMap)
        val bGcMap = rdd.sparkContext.broadcast(gcMap)
        val bScMap = rdd.sparkContext.broadcast(scMap)
        val bBscMap = rdd.sparkContext.broadcast(bscMap)

        //      println("agent game channel size is "+gcMap.size+".")
        //      Save2Kafka.saveMetrics(JunhaiLog.MONITOR_TOPIC,new Date(),DataProcess,JunhaiLog.INFO,s"Total get from rocketMQ $nameServer and topic(s) $topicsStr","Num",rdd.count()+"")
        println(topic)

        val document = rdd.distinct().map(string => {
          try {
            val doc = Document.parse(string)
            (doc, true)
          } catch {
            case e: Exception =>
              System.err.println(new Date() + " Error: json parse error!")
              (new Document(LOG, string), false)
          }
        })

        val flat = document.filter(line => line._2 && line._1.isEmpty).flatMap(line => {
          line._1.get(rows).asInstanceOf[util.ArrayList[Document]].map(doc => {
            (doc.get(data).asInstanceOf[Document], doc.getString(database), doc.getString(table), doc.getString(actionType))
          })
        })

        //      val copyTable=flat.filter(_._3==agent_order_copy).map(line=>{
        //        (line._1.getLong(order_sn),line._1.getString(create_ip))
        //      }).collect().toMap
        //      val bCopyMap=rdd.sparkContext.broadcast(copyTable)
        val copyTable = flat.filter(_._3 == agent_order_copy)

        Save2Kafka.saveDStreamAsync(copyTable.map(_._1.toJson()), kafkaSink, topic + "Copy")

        val resultDocuments: RDD[Document] = flat.filter(_._3 == agent_order).map(order => {
          val orderSn = order._1.getLong(order_sn)
          //        if(bCopyMap.value.nonEmpty){
          //          order._1.append(create_ip,bCopyMap.value.get(orderSn,"0.0.0.0"))
          //        }
          //开始关联
          val gameId = order._1.getInteger(game_id)
          val columnValue = order._1.getInteger(column) + "" //默认的独代的channel_id字段
          if (gameId >= 63) {
            //此时order表中的channel_id存放的是game_channel_id的值 需要得到channel_id
            val value = bGgMap.value.getOrElse(gameId + columnValue, error) //获得channel_id
            val subPf = bAcMap.value.getOrElse(value, error)
            order._1.put(channel_id, value)
            order._1.put(game_channel_id, columnValue) //将原来的字段名字改回来
            order._1.put(sub_pf, subPf)
          } else {
            if (!bScMap.value.contains(columnValue)) {
              //此时order表中的channel_id就是channel_id
              val value = bGcMap.value.getOrElse(gameId + columnValue, error) //拿到game_channel_id
              val subPf = bAcMap.value.getOrElse(columnValue, error)
              order._1.put(game_channel_id, value)
              order._1.put(sub_pf, subPf)
            } else {
              val value = bBscMap.value.getOrElse(columnValue, error) //根据sub_channel拿game_channel_id
              order._1.put(game_channel_id, value)
              val channelId = bScMap.value.getOrElse(columnValue, error)
              order._1.put(channel_id, channelId)
              val subPf = bAcMap.value.getOrElse(channelId, error)
              order._1.put(sub_pf, subPf)
            }
          }
          order._1.put(order_sn, orderSn + "")
          order._1
        }).cache()
        val withNewUserFlag = Save2Redis.saveDatabaseOrder(resultDocuments, "agent", redisSinkCluster)
          .map(document => {
            KafkaLogProcess.documentFlat("", document)
          })
        Save2Kafka.saveDStreamSync(withNewUserFlag.map(_.toJson()), kafkaSink, topic, 8)
        //      Save2Kafka.saveDStreamTransaction(withNewUserFlag.map(_.toJson()),topic,topics,offset,nameServer,group)
        //      withNewUserFlag.foreach(println)
        resultDocuments.unpersist()
      }
    })
  }

  /**
    * 订单关联处理
    */
  def oldIOSAndHW(ds: DStream[String]
                  , kafkaSink: Broadcast[KafkaSink]
                  , column: String
                  , topic: String
                  , hdfsCachePath: String
                  , system: String
                  , redisSinkCluster: Broadcast[RedisSink]): Unit = {

    println("------------" + System.currentTimeMillis() + "------------")
    ds.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val rows = "rows"
        val table = "table"
        val data = "data"
        val order = "order"
        val user = "user"
        val game_id = "game_id"
        val jh_app_id = "jh_app_id"
        val INSERT = "INSERT"

        val jh_channel = "jh_channel"
        val iosGame = Set("100000297", "100000332", "100000344", "100000345", "100000343")
        val iosChannel = Set("appstore", "beautyIAPH5", "greatIAPH5", "iOSAlipayH5", "iOSWeiXinPayH5", "wapPay")
        val gcgTuple = AgentProcess.getAgentGameChannelForChannel(hdfsCachePath)
        val bGcgMap = rdd.sparkContext.broadcast(gcgTuple)

        val gcmMap = AgentProcess.getAgentGame(hdfsCachePath)
        val bGcmMap = rdd.sparkContext.broadcast(gcmMap)

        val gameChannel = AgentProcess.getAgent2GameChannel(hdfsCachePath)
        val bGameChannel = rdd.sparkContext.broadcast(gameChannel)

        val document = rdd.distinct().map(string => {
          try {
            val doc = Document.parse(string)
            (doc, true)
          } catch {
            case e: Exception =>
              System.err.println(new Date() + " Error: json parse error!")
              (new Document(LOG, string), false)
          }
        })

        val filterDocuments = document.filter(_._2).flatMap(line => {
          line._1.get(rows).asInstanceOf[util.ArrayList[Document]].map(doc => {
            val actionType = doc.getString("type")
            doc.getString(table) match {
              case `order` =>
                (doc.get(data).asInstanceOf[Document], doc.getString(game_id), order, doc.getString(jh_channel), actionType)
              case `user` =>
                (doc.get(data).asInstanceOf[Document], doc.getString(jh_app_id), user, doc.getString(jh_channel), actionType)
              case _ =>
                (new Document(), "", "", "", actionType)
            }
          })
        })

        val resultDocuments = AgentProcess.orderTransform(filterDocuments.map(_._1), system, game_id, bGcgMap, bGameChannel, bGcmMap)
          .union(AgentProcess.registerTransform(filterDocuments.map(_._1), jh_app_id, bGameChannel, bGcmMap)).cache()

        val withNewUserFlag = Save2Redis.saveDatabaseOrder(resultDocuments, "agent", redisSinkCluster)
          .map(document => {
            KafkaLogProcess.documentFlat("", document)
          })
        Save2Kafka.saveDStreamSync(withNewUserFlag.map(_.toJson()), kafkaSink, topic, 8)

        resultDocuments.unpersist()
      }
    })
  }


  //独代登录、订单   海外登录、订单
  def kylinProcess(ds: DStream[String], kafkaSink: Broadcast[KafkaSink], descTopic: String): Unit = {
    ds.foreachRDD(rdd => {
      rdd.foreach(log => {
        var boolean = true
        var topic = ""
        val doc = try {
          Document.parse(log)
        } catch {
          case _: Exception =>
            boolean = false
            new Document()
        }
        boolean = doc.getString(JunhaiLog.is_test) == "regular"
        val finalDoc = JunhaiLog.getString(doc, JunhaiLog.event) match {
          case JunhaiLog.eventLogin =>
            topic = descTopic.split(",")(0)
            docParser(doc)
          case JunhaiLog.eventOrder =>
            topic = descTopic.split(",")(1)
            if (JunhaiLog.getInt(doc, JunhaiLog.pay_status) == 1 && JunhaiLog.getInt(doc, JunhaiLog.status) == 2) {
              docParser(doc, false).append(JunhaiLog.money, JunhaiLog.getDouble(doc, JunhaiLog.money))
            } else {
              boolean = false
              new Document()
            }
          case _ =>
            boolean = false
            new Document()
        }
        if (boolean) {
          kafkaSink.value.send(topic, finalDoc.toJson())
        }
      })
    })
  }

  def docParser(document: Document, boolean: Boolean = true): Document = {
    val date = "server_date_day"
    val hour = "server_date_hour"
    val doc = new Document().append(date, document.getString(date))
      .append(hour, document.getString(hour))
      .append(JunhaiLog.reg_date, document.getString(JunhaiLog.reg_date))
      .append(JunhaiLog.first_order_date, document.getString(JunhaiLog.first_order_date))

    if (boolean) {
      doc.append(JunhaiLog.game_id, JunhaiLog.getString(document, "game-game_id"))
        .append(JunhaiLog.channel_id, JunhaiLog.getString(document, "agent-channel_id"))
        .append(JunhaiLog.game_channel_id, JunhaiLog.getString(document, "agent-game_channel_id"))
        .append(JunhaiLog.os_type, JunhaiLog.getString(document, "device-os_type"))
        .append(JunhaiLog.user_id, JunhaiLog.getString(document, "user-user_id"))
    } else {
      doc.append(JunhaiLog.game_id, JunhaiLog.getString(document, JunhaiLog.game_id))
        .append(JunhaiLog.channel_id, JunhaiLog.getString(document, JunhaiLog.channel_id))
        .append(JunhaiLog.game_channel_id, JunhaiLog.getString(document, JunhaiLog.game_channel_id))
        .append(JunhaiLog.os_type, JunhaiLog.getString(document, JunhaiLog.os_type))
        .append(JunhaiLog.user_id, JunhaiLog.getString(document, JunhaiLog.user_id))
    }

  }
}
