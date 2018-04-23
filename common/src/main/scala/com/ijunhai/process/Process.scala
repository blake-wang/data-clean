package com.ijunhai.process

import java.util

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.monitor.MonitorType.MonitorType
import com.ijunhai.process.agent.AgentProcess.FORMAT_STR
import com.ijunhai.process.agent.ChannelTransform.HWGameMap
import com.ijunhai.process.agent.KafkaLogProcess._
import org.apache.spark.broadcast.Broadcast
import org.bson.Document
import com.ijunhai.common.monitor.{Monitor, MonitorType}

import scala.collection.JavaConversions._


/**
  * Created by admin on 2018/3/28.
  */
object Process extends Serializable {
  val error = "error"
  val firstStr = "NORMAL: ["
  val secondStr = "] CIP["

  def getData(Str: String, thirdStr: String, append: Boolean = false): (Document, Boolean) = {
    try {
      val doc = Document.parse(Str)
      val msg = JunhaiLog.getString(doc, "message")
      if (append) { //是否获得ip和ts
        val time = msg.substring(msg.indexOf(firstStr) + firstStr.length, msg.indexOf(secondStr))
        (Document.parse(msg.substring(msg.indexOf(thirdStr) + thirdStr.length, msg.length - 1))
          .append("client_ip", msg.substring(msg.indexOf(secondStr) + secondStr.length, msg.indexOf(thirdStr) - 2))
          .append("server_ts", TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", time, TimeUtil.SECOND).toInt), true)
      } else {
        val json_str = msg.substring(msg.indexOf(thirdStr) + thirdStr.length, msg.length - 1)
        (Document.parse(json_str), true)
      }
    } catch {
      case _: Exception =>
        (new Document().append("msg", Str), false)
    }
  }

  def getDalanData(msg: String, thirdStr: String, append: Boolean = false): (Document, Boolean) = {
    try {
      if (append) { //是否获得ip和ts
        val time = msg.substring(msg.indexOf(firstStr) + firstStr.length, msg.indexOf(secondStr))
        (Document.parse(msg.substring(msg.indexOf(thirdStr) + thirdStr.length, msg.length - 1))
          .append("client_ip", msg.substring(msg.indexOf(secondStr) + secondStr.length, msg.indexOf(thirdStr) - 2))
          .append("server_ts", TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", time, TimeUtil.SECOND).toInt), true)
      } else {
        val json_str = msg.substring(msg.indexOf(thirdStr) + thirdStr.length, msg.length - 1)
        (Document.parse(json_str), true)
      }
    } catch {
      case _: Exception =>
        (new Document().append("msg", msg), false)
    }
  }

  def getOrderData(Str: String): (Document, Boolean) = {
    try {
      val doc = Document.parse(Str)
      val msg = JunhaiLog.getString(doc, "message")

      (Document.parse(msg), true)

    } catch {
      case _: Exception =>
        (new Document().append("msg", Str), false)
    }
  }


  def loginFormat(log: String, source: String
                  , bDataCleanConfig: Broadcast[Document]
                  , ipClass: IP
                 ): (Document, Boolean) = {
    var result = true
    var jsonParse = true
    var flag = true
    val document =
      source match {
        case "agentLoginSrc" =>
          val info: (Document, Int, String) = try {
            val doc = Document.parse(log)
            val msg = JunhaiLog.getString(doc, "message")
            JunhaiLog.agentOldLog2Bson(msg)
          } catch {
            case _: Exception =>
              jsonParse = false
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
        case "dalanLoginSrc" =>
          getDalanData(log, "DATA[")
        case _ =>
          getData(log, "DATA[")

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
                }
              })
            }
          })
        }
      }
      addRegion(doc, ipClass) //添加ip地域
    }
    result = if (JunhaiLog.getString(doc, JunhaiLog.event) != JunhaiLog.eventLogin) false else result
    (doc, result)
  }

  def loginJoin(doc: Document, source: String,
                bAcMap: Broadcast[Map[String, String]],
                bGgMap: Broadcast[Map[String, String]],
                bGcMap: Broadcast[Map[String, String]],
                bScMap: Broadcast[Map[String, String]],
                bBscMap: Broadcast[Map[String, String]],
                bGcmMap: Broadcast[Map[String, String]],
                bGameChannel: Broadcast[Map[String, (String, String, String)]],
                bRmb: Broadcast[Map[String, Double]],
                bUsd: Broadcast[Map[String, Double]],
                bIOS128: Broadcast[Set[String]], booleanB: Boolean = true
               ): (Document, Boolean) = {
    if (booleanB) {
      var boolean = true
      source match {
        case "haiwaiLoginSrc" =>
          boolean = doc.get(JunhaiLog.channel_platform) != null &&
            JunhaiLog.getSecondColumnString(doc, JunhaiLog.channel_platform, JunhaiLog.ad_id) != "" &&
            JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.game_id) != ""
          val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
          val agent = JunhaiLog.getDocument(doc, JunhaiLog.agent)
          var belongGameId = JunhaiLog.getString(game, JunhaiLog.game_id)
          belongGameId = HWGameMap.getOrElse(belongGameId, belongGameId)
          val (gameChannelId, channelId, gameId) = bGameChannel.value.getOrElse(belongGameId, (error, error, error))
          game.put("app_id", gameId) //这条语句必须要在上面
          game.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId, error))
          agent.put(JunhaiLog.channel_id, channelId)
          agent.put(JunhaiLog.game_channel_id, gameChannelId)
          doc.put(JunhaiLog.agent, agent)
          doc.put(JunhaiLog.game, game)
          changePhase(doc)
        case "dalanLoginSrc" =>
          val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
          val agent = JunhaiLog.getDocument(doc, JunhaiLog.agent)
          val columnValue = JunhaiLog.getString(game, JunhaiLog.game_id) //默认的独代的channel_id字段
        val values = bGameChannel.value.getOrElse(columnValue, (error, error, error))
          val gameChannelId = values._1
          val channelId = values._2
          val gameId = values._3
          game.put("app_id", gameId)
          game.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId, error))
          agent.put(JunhaiLog.channel_id, channelId)
          agent.put(JunhaiLog.game_channel_id, gameChannelId)
          doc.put(JunhaiLog.agent, agent)
          doc.remove(JunhaiLog.game)
          doc.put(JunhaiLog.game, game)
          if ((!bIOS128.value.contains(gameChannelId)) && doc.getString(JunhaiLog.event).equals("login")) { //过滤出小于128的ios独代
            (doc, false)
          } else {
            changePhase(doc)
          }
        case "agentLoginSrc" =>
          val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
          val gameId = game.getString(JunhaiLog.game_id)
          val device = doc.get(JunhaiLog.device).asInstanceOf[Document]
          boolean = doc.get(JunhaiLog.agent) != null &&
            gameId != "" && !(doc.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id) == ""
            && doc.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.game_channel_id) == "")
          val agent = doc.get(JunhaiLog.agent).asInstanceOf[Document]
          val channelId = agent.getString(JunhaiLog.channel_id)
          val gameChannelId = agent.getString(JunhaiLog.game_channel_id)
          if (boolean) {
            if (channelId.equals("")) {
              val value = bGgMap.value.getOrElse(gameId + gameChannelId, error)
              agent.put(JunhaiLog.channel_id, value)
            } else if (gameChannelId.equals("") || gameChannelId.equals("0")) {
              //   独代日志 gameid>=63，gameChannelId为空的已经在前面全部过滤了
              if (!bScMap.value.contains(channelId)) {
                //http://game.data.ijunhai.com/Gamedata/api/getAgentSubChannel
                //此时order表中的channel_id就是channel_id
                val value = bGcMap.value.getOrElse(gameId + channelId, error) //拿到game_channel_id
                val pf = bAcMap.value.getOrElse(channelId, error)
                agent.put(JunhaiLog.game_channel_id, value)
                doc.put(JunhaiLog.os_type, pf)
              } else {
                //有子渠道subchannel的关联
                //http://game.data.ijunhai.com/Gamedata/api/getAgentOldGameChannel
                val agentChannelId = bScMap.value.getOrElse(channelId, error)
                agent.put(JunhaiLog.channel_id, agentChannelId)
                val value = bBscMap.value.getOrElse(channelId, error) //此时的channel_id中方的是sub_channel_id的值根据sub_channel拿game_channel_id
                if (value.equals(error)) {
                  //出现subchannelId匹配不到的情况，则直接使用agentchannelId去匹配得到gamechannelid
                  val agentValue = bGcMap.value.getOrElse(gameId + agentChannelId, error)
                  agent.put(JunhaiLog.game_channel_id, agentValue)
                } else {
                  agent.put(JunhaiLog.game_channel_id, value)
                }
                val pf = bAcMap.value.getOrElse(agentChannelId, error)
                device.put(JunhaiLog.os_type, pf)
              }
            }
            val companyId = bGcmMap.value.getOrDefault(gameId, error) //游戏公司关联
            game.put(JunhaiLog.company_id, companyId)
          }
          //剔除haiwai渠道的数据
          boolean = !JunhaiLog.haiwaiChannelId.contains(doc.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id))
          (doc, boolean)
        case _ =>
          (new Document, false)
      }
    } else {
      (doc, false)
    }
  }

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
  val os_type = "os_type"

  def orderJoin(doc: Document, source: String, bAcMap: Broadcast[Map[String, String]],
                bGgMap: Broadcast[Map[String, String]], bGcMap: Broadcast[Map[String, String]],
                bScMap: Broadcast[Map[String, String]], bBscMap: Broadcast[Map[String, String]],
                bGcmMap: Broadcast[Map[String, String]], bGameChannel: Broadcast[Map[String, (String, String, String)]],
                bRmb: Broadcast[Map[String, Double]], bUsd: Broadcast[Map[String, Double]], bIOS128: Broadcast[Set[String]],
                bGcgMap: Broadcast[Map[String, (String, String)]]): (Document, Boolean) = {
    var boolean = true
    source match {
      case "agentDBSrc" =>
        val gameId = JunhaiLog.getInt(doc, game_id)
        if (gameId != -1) {
          val columnValue = JunhaiLog.getInt(doc, channel_id) + "" //默认的独代的channel_id字段
          val channelId = if (gameId >= 63) {
            //此时order表中的channel_id存放的是game_channel_id的值 需要得到channel_id
            val value = bGgMap.value.getOrElse(gameId + columnValue, error) //bGgMap 独代game_id + game_channel_id 获得 channel_id
            doc.put(channel_id, value)
            doc.put(game_channel_id, columnValue) //将原来的字段名字改回来
            value
          } else {
            if (!bScMap.value.contains(columnValue)) {
              //此时order表中的channel_id就是channel_id
              val value = bGcMap.value.getOrElse(gameId + columnValue, error) //拿到game_channel_id
              doc.put(game_channel_id, value)
              columnValue
            } else {
              val value = bBscMap.value.getOrElse(columnValue, error) //根据sub_channel拿game_channel_id
              val channelId = bScMap.value.getOrElse(columnValue, error)
              doc.put(channel_id, channelId)
              if (value.equals(error)) {
                val agentValue = bGcMap.value.getOrElse(gameId + channelId, error)
                doc.put(JunhaiLog.game_channel_id, agentValue)
              } else {
                doc.put(JunhaiLog.game_channel_id, value)
              }
              channelId
            }
          }

          doc.append(os_type, bAcMap.value.getOrElse(channelId, error))
            .append(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId + "", error))
            .append(JunhaiLog.server_ts, JunhaiLog.getTimestamp(doc, JunhaiLog.update_time))
            .append(JunhaiLog.update_time, JunhaiLog.getTimestamp(doc, JunhaiLog.update_time))
            .append(JunhaiLog.event, "order").append(JunhaiLog.is_test, "regular")


        } else {
          boolean = false
        }
        (doc, boolean)
      case "haiwaiDBSrc" =>
        val (afterJoin, boolean) = orderTrans(doc, source, bAcMap, bGcgMap, bGameChannel, bGcmMap)
        if (boolean) {
          val currencyAmount = JunhaiLog.getDouble(afterJoin, JunhaiLog.money)
          val currencyType = JunhaiLog.getString(afterJoin, JunhaiLog.currency_type)
          afterJoin.append(JunhaiLog.cny_amount, bRmb.value.getOrDefault(currencyType, 1.0) * currencyAmount)
            .append(JunhaiLog.usd_amount, bUsd.value.getOrDefault(currencyType, 1.0) * currencyAmount)
          (afterJoin, boolean)
        } else {
          (afterJoin, boolean)
        }

      case "dalanDBSrc" =>
        val (afterJoin, tmpBoolean) = orderTrans(doc, source, bAcMap, bGcgMap, bGameChannel, bGcmMap)
        if (!bIOS128.value.contains(JunhaiLog.getString(afterJoin, JunhaiLog.game_channel_id))) {
          boolean = false //剔除不在集合（小于128的iOS独代数据）中的包
        }
        (afterJoin, boolean && tmpBoolean)
    }
  }

  def orderTrans(doc: Document, system: String
                 , bAcMap: Broadcast[Map[String, String]]
                 , bGameChannelIdMap: Broadcast[Map[String, (String, String)]]
                 , bGameChannel: Broadcast[Map[String, (String, String, String)]]
                 , bGcmMap: Broadcast[Map[String, String]]
                 , combineFlag: Boolean = true
                ): (Document, Boolean) = {
    val gameId = JunhaiLog.getString(doc, game_id)
    if (gameId != "") { //order表中不存在jh_app_key user表中不存在game_id
      val junhaiLog = new JunhaiLog()
      if (combineFlag) { //order
        val columnValue = JunhaiLog.getString(doc, game_id) //此时game_id为belong_game_id
      val (gameChannelId, channelId, gameId) = bGameChannel.value.getOrDefault(columnValue, (error, error, error))
        junhaiLog.game_channel_id = gameChannelId
        junhaiLog.channel_id = channelId
        junhaiLog.game_id = gameId
      } else { //order_jh
        val columnValue = JunhaiLog.getString(doc, "jh_channel") // 此时jh_channel为game_channel_id
      val (channelId, gameId) = bGameChannelIdMap.value.getOrDefault(columnValue, (error, error))
        junhaiLog.game_channel_id = columnValue
        junhaiLog.channel_id = channelId
        junhaiLog.game_id = gameId
      }
      junhaiLog.os_type = bAcMap.value.getOrElse(junhaiLog.channel_id, "") //根据channelId获得subpf
      junhaiLog.order_sn = JunhaiLog.getString(doc, JunhaiLog.order_sn)
      junhaiLog.user_id = JunhaiLog.getString(doc, JunhaiLog.user_id)
      junhaiLog.currency_amount = JunhaiLog.getDouble(doc, "money")
      junhaiLog.currency_type = JunhaiLog.getString(doc, "money_type")
      junhaiLog.pay_status = JunhaiLog.getString(doc, "pay_status")
      junhaiLog.status = JunhaiLog.getString(doc, "status")
      junhaiLog.sub_pf = junhaiLog.os_type

      //      val status = JunhaiLog.getInt(doc, "status")
      //      val newStatus = status match {
      //        case 1 => //已下单
      //          "create_order"
      //        case 2 => //已付费
      //          "paid_success"
      //        case 3 => //通知CP失败
      //          "notify_cp_failue"
      //        case 4 => //通知CP成功
      //          "notify_cp_success"
      //        case 5 => //CP处理完成
      //          "finish"
      //        case _ =>
      //          status + ""
      //      }
      //      junhaiLog.order_step = newStatus
      //      val payStatus = JunhaiLog.getInt(doc, "pay_status")
      //      val newPayStatus = payStatus match {
      //        case 0 => //失败
      //          "failure"
      //        case 1 => //成功
      //          "success"
      //        case _ =>
      //          payStatus + ""
      //      }
      //      junhaiLog.order_status = newPayStatus

      junhaiLog.device_id = JunhaiLog.getString(doc, JunhaiLog.device_id)
      junhaiLog.role_server = JunhaiLog.getString(doc, "server")
      val payType = JunhaiLog.getIntString(doc, "pay_type")
      val newPayType = if (system.contains(JunhaiLog.HAIWAI)) {
        payType match {
          case "1" =>
            "google_pay"
          case "2" =>
            "mycard"
          case "3" =>
            "visa_global"
          case "4" =>
            "apple_pay"
          case "5" =>
            "mol"
          case "6" =>
            "huawei"
          case "7" =>
            "onestore"
          case _ =>
            payType
        }
      } else {
        payType match {
          case "1" | "9" | "10" | "14" => //支付宝
            "alipay"
          case "6" | "12" | "13" | "15" | "17" | "18" | "19" | "21" | "22" => //微信
            "weichat_pay"
          case "5" => //易宝
            "yeebao"
          case "7" => //苹果
            "apple_pay"
          case "8" => //爱贝
            "iapppay"
          case "20" => //银联
            "unionpay"
          case _ =>
            payType
        }
      }

      junhaiLog.order_type = newPayType
      junhaiLog.cp_trade_sn = if (doc.getString(JunhaiLog.cp_trade_sn) == null) "" else doc.getString(JunhaiLog.cp_trade_sn)
      junhaiLog.channel_trade_sn = if (doc.getString("payment_trade_sn") == null) "" else doc.getString("payment_trade_sn")
      junhaiLog.role_id = if (doc.getString("game_role_id") == null) "" else doc.getString("game_role_id")
      junhaiLog.role_name = if (doc.getString("game_role_name") == null) "" else doc.getString("game_role_name")
      junhaiLog.server_ts = JunhaiLog.getTimestamp(doc, "update_time", FORMAT_STR)
      junhaiLog.client_ip = JunhaiLog.getString(doc, "create_ip")
      junhaiLog.game_ver = JunhaiLog.getString(doc, "application_version")
      junhaiLog.sdk_ver = JunhaiLog.getString(doc, "sdk_version")
      junhaiLog.android_imei = JunhaiLog.getString(doc, "imei")
      junhaiLog.event = "order"
      val result = JunhaiLog.getFlatOrder(junhaiLog) //构成整条数据

      result.append(JunhaiLog.APP_KEY, JunhaiLog.getString(doc, game_id))
      result.append(JunhaiLog.company_id, bGcmMap.value.getOrDefault(junhaiLog.game_id, error))
      (result, true)
    } else {
      (new Document().append("msg", doc), false)
    }

  }

  def getTopicName(source: String, tableName: String, log: Document, monitor: Monitor, bIOS128: Broadcast[Set[String]], filterRule: Broadcast[Map[String, (util.ArrayList[String], util.ArrayList[Document])]]): (MonitorType, String) = {
    val dest = source.substring(0, source.length - 3)
    tableName match {
      case "agent_order_copy" =>
        monitor.add(MonitorType.COPY)
        (MonitorType.COPY, dest + "Copy")
      case _ =>
        if (JunhaiLog.haiwaiChannelId.contains(JunhaiLog.getString(log, JunhaiLog.channel_id))) {
          monitor.add(MonitorType.FILTER)
          (MonitorType.FILTER, dest + "Filter")
        } else if (Process.isTestOrder(log, filterRule)) {
          monitor.add(MonitorType.TEST)
          (MonitorType.TEST, dest + "Test")
        } else if (bIOS128.value.contains(JunhaiLog.getString(log, JunhaiLog.game_channel_id))) {
          monitor.add(MonitorType.OTHER)
          (MonitorType.OTHER, dest + "Other")
        } else {
          monitor.add(MonitorType.DEST)
          (MonitorType.DEST, dest)
        }
    }
  }


  def isTestOrder(doc: Document, filterRule: Broadcast[Map[String, (util.ArrayList[String], util.ArrayList[Document])]]): Boolean = {
    var isTest = false //非测试订单
    val gameId = JunhaiLog.getString(doc, "game_id")
    if (filterRule.value.containsKey(gameId)) {
      val server = JunhaiLog.getString(doc, "server")
      val (listA, listB) = filterRule.value.getOrDefault(gameId, (new util.ArrayList[String], new util.ArrayList[Document]))
      if (listA.contains(server)) {
        isTest = true
      } else if (!listB.isEmpty) {
        for (doc <- listB) {
          val start = JunhaiLog.getInt(doc, "start")
          val end = JunhaiLog.getInt(doc, "end")
          if (server.toInt >= start && server.toInt <= end) {
            isTest = true
          }
        }
      }
    }
    isTest
  }

  //IP关联地域
  def addRegion(doc: Document, ip: IP): Unit = {
    try {
      if (doc.getString(JunhaiLog.client_ip) != "") {
        val clientIp = JunhaiLog.getString(doc, JunhaiLog.client_ip)
        val geo: util.HashMap[String, String] = IPAddress.getIPAddress(if (clientIp == "") doc.getString(JunhaiLog.create_ip) else clientIp, ip)
        for (key <- geo.keySet()) {
          var value: String = geo.get(key)
          if (value.contains("\"")) {
            value = value.replaceAll("\"", "")
          }
          doc.put(key, value)
        }
      }
    } catch {
      case _: Exception =>
        doc.put("province", "")
        doc.put("country", "")
        doc.put("city", "")
    }
  }

  def changePhase(doc: Document): (Document, Boolean) = {
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
    if (newGame.getString(JunhaiLog.app_id).equals("100000011")) { //海外的特殊belong_game_id，走网页支付，直接过滤掉
      (document, false)
    } else {
      (document, true)
    }
  }
}
