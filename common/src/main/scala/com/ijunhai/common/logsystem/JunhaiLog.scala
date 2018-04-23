package com.ijunhai.common.logsystem

import com.ijunhai.common.TimeUtil
import org.bson.Document

import scala.collection.JavaConversions._

/**
  * Created by Admin on 2017-08-14.
  */
class JunhaiLog {

  //一级标签
  var user = ""
  var game = ""
  var agent = ""
  var device = ""
  var role = ""
  var mission = ""
  var copy = ""
  var trade = ""
  var order = ""
  var click = ""
  var custom = ""
  var channel_platform = ""
  var junhai_sdk = ""

  var sub_pf = ""

  var status = ""
  var server_first_date = ""
  var is_new_user = ""
  var server_date = ""
  var event = ""
  var is_test = "regular"
  var data_ver = "1.0"
  var client_time_zone = "+08:00"
  var client_ts = 0
  var server_time_zone = "+08:00"
  var server_ts = 0
  var client_ip = ""
  var country = ""
  var province = ""
  var city = ""
  var area_code = ""
  var remark = ""

  //二级标签
  var user_id = ""
  var user_name = ""
  var gender = ""
  var birth = ""
  var age = ""
  var phone_num = ""
  var email_num = ""
  var login_type = ""
  var open_id = ""

  var game_id = ""
  var game_name = ""
  var game_ver = ""

  var channel_id = ""
  var game_channel_id = ""
  var access_token = ""

  var screen_height = ""
  var screen_width = ""
  var device_id = ""
  var ios_idfa = ""
  var android_imei = ""
  var android_adv_id = ""
  var android_id = ""
  var device_name = ""
  var os_ver = ""
  var sdk_ver = ""
  var package_name = ""
  var os_type = ""
  var net_type = ""
  var user_agent = ""
  var pay_status = ""
  var role_level = ""
  var role_name = ""
  var role_server = ""
  var role_id = ""
  var role_type = ""
  var role_gender = ""
  var association_id = ""
  var association_name = ""
  var association_rank = ""

  var mission_id = ""
  var mission_name = ""
  var mission_type = ""
  var mission_process = ""
  var mission_step = ""

  var copy_id = ""
  var copy_name = ""
  var copy_level = ""
  var copy_diffculty = ""
  var copy_process = ""

  var trade_type = ""
  var trade_amount = ""
  var remain_amount = ""
  var item_name = ""
  var item_amount = ""
  var trade_id = ""
  var trade_desc = ""
  var is_bind = ""

  var order_sn = ""
  var cp_trade_sn = ""
  var channel_trade_sn = ""
  var request_url = ""
  var http_code = ""
  var request_result = ""
  var order_step = ""
  var cny_amount = ""
  var currency_amount = 0.0
  var currency_type = ""
  var usd_amount = ""
  var order_status = ""
  var order_type = ""


  var event_tag = ""
  var click_time = 0

  var ad_id = ""

  var app_id = ""
  val imei = ""

}

object JunhaiLog {

  val agentLoginSrc = "agentLoginSrc"
  val agentOldLoginSrc = "agentOldLoginSrc"
  val agentDBSrc = "agentDBSrc"
  val dalanLoginSrc = "dalanLoginSrc"
  val dalanDBSrc = "dalanDBSrc"
  val haiwaiLoginSrc = "haiwaiLoginSrc"
  val haiwaiDBSrc = "haiwaiDBSrc"
  val COPY = "Copy"
  val TEST = "Test"
  val OTHER = "Other"
  val FILTER = "Filter"


  val MONITOR_TOPIC = "monitor"
  val LOG = "log"
  val DATA = "data"
  val junhaiChannelsId = Set("199", "8084", "10033", "10081", "10082", "10085", "10086", "10087",
    "10088", "10089", "10099", "10100", "10107", "10133", "10150", "10151", "10157", "10162", "6001") //属于大蓝和海外渠道
  val haiwaiChannelId = Set("6001", "10081", "10082", "10085", "10086", "10087", "10088", "10089", "10099", "10100", "10107")
  val dalanChannelId = Set("199", "8084", "10033", "10133", "10150", "10151", "10157", "10162")
  val DALAN = "dalan"
  val SHENQI = "shenqi"
  val LIANYUN = "lianyun"
  val HAIWAI = "haiwai"
  val CHUMENG = "chumeng"
  val channelsWithOutDalan = Set(SHENQI, LIANYUN, HAIWAI, CHUMENG)
  val junhaiChannelsName = Set(DALAN, SHENQI, LIANYUN, HAIWAI, CHUMENG)
  val agentName = Set(DALAN, "agent")

  val APP_KEY = "app_key" //由于独代和自有渠道数据混合，导致game下的gameID字段不够用，添加该字段用来表示自有渠道的游戏ID
  //日志级别
  val ERROR = "ERROR"
  val WARM = "WARM"
  val INFO = "INFO"
  //event
  val eventLogin = "login"
  val eventOrder = "order"
  val eventActive = "active"
  val eventRegister = "register"
  val eventCrash = "crash"
  //service
  val serviceSDK = "sdk"
  val serviceAgent = "agent"
  val serviceChannel = "channel"
  val serviceDalan = "dalan"

  //一级标签
  val user = "user"
  val game = "game"
  val agent = "agent"
  val device = "device"
  val role = "role"
  val mission = "mission"
  val copy = "copy"
  val trade = "trade"
  val order = "order"
  val click = "click"
  val custom = "custom"
  val channel_platform = "channel_platform"
  val junhai_sdk = "junhai_sdk"

  val reg_date = "reg_date"
  val first_order_date = "first_order_date"
  val server_date = "server_date"

  val status = "status"
  val event = "event"
  val is_test = "is_test"
  val data_ver = "data_ver"
  val client_time_zone = "client_time_zone"
  val client_ts = "client_ts"
  val server_time_zone = "server_time_zone"
  val server_ts = "server_ts"
  val client_ip = "client_ip"
  val create_ip = "create_ip"

  val country = "country"
  val province = "province"
  val city = "city"
  val area_code = "area_code"
  val remark = "remark"
  //二级标签
  val user_id = "user_id"
  val user_name = "user_name"
  val gender = "gender"
  val birth = "birth"
  val age = "age"
  val phone_num = "phone_num"
  val email_num = "email_num"
  val login_type = "login_type"
  val open_id = "open_id"

  val pay_status = "pay_status"
  val update_time = "update_time"

  val game_id = "game_id"
  val game_name = "game_name"
  val game_ver = "game_ver"
  val company_id = "company_id"
  val channel_id = "channel_id"
  val game_channel_id = "game_channel_id"
  val access_token = "access_token"
  val screen_height = "screen_height"
  val screen_width = "screen_width"
  val device_id = "device_id"
  val ios_idfa = "ios_idfa"
  val android_imei = "android_imei"
  val android_adv_id = "android_adv_id"
  val android_id = "android_id"
  val device_name = "device_name"
  val os_ver = "os_ver"
  val sdk_ver = "sdk_ver"
  val package_name = "package_name"
  val os_type = "os_type"
  val net_type = "net_type"
  val user_agent = "user_agent"
  val role_level = "role_level"
  val role_name = "role_name"
  val role_server = "role_server"
  val role_id = "role_id"
  val role_type = "role_type"
  val role_gender = "role_gender"
  val association_id = "association_id"
  val association_name = "association_name"
  val association_rank = "association_rank"
  val mission_id = "mission_id"
  val mission_name = "mission_name"
  val mission_type = "mission_type"
  val mission_process = "mission_process"
  val mission_step = "mission_step"
  val copy_id = "copy_id"
  val copy_name = "copy_name"
  val copy_level = "copy_level"
  val copy_diffculty = "copy_diffculty"
  val copy_process = "copy_process"
  val trade_type = "trade_type"
  val trade_amount = "trade_amount"
  val remain_amount = "remain_amount"
  val item_name = "item_name"
  val item_amount = "item_amount"
  val trade_id = "trade_id"
  val trade_desc = "trade_desc"
  val is_bind = "is_bind"
  val order_sn = "order_sn"
  val cp_trade_sn = "cp_trade_sn"
  val channel_trade_sn = "channel_trade_sn"
  val request_url = "request_url"
  val http_code = "http_code"
  val request_result = "request_result"
  val order_step = "order_step"
  val cny_amount = "cny_amount"
  val currency_amount = "currency_amount"
  val currency_type = "currency_type"
  val usd_amount = "usd_amount"
  val order_status = "order_status"
  val order_type = "order_type"
  val event_tag = "event_tag"
  val click_time = "click_time"
  val ad_id = "ad_id"
  val app_id = "app_id"
  val event_type = "event_type"
  val event_order = "event_order"
  val eventAf = "install"
  val imei = "imei"
  val money = "money"

  def getString(document: Document, key: String): String = {
    try {
      document.get(key) match {
        case value: Integer =>
          value + ""
        case value: String =>
          value
        case value: java.lang.Long =>
          value + ""
        case value: java.lang.Double =>
          String.valueOf(value)
        case _ =>
          //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
          ""
      }
    } catch {
      case e: Exception =>
        ""
    }
  }

  def getDoubleString(document: Document, key: String): String = {
    document.get(key) match {
      case value: Integer =>
        value + ".0"
      case value: String =>
        if (value.contains(".")) value else value + ".0"
      case value: java.lang.Long =>
        value + ".0"
      case value: java.lang.Double =>
        String.valueOf(value)
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        "0.0"
    }
  }

  def getDouble(document: Document, key: String): Double = {
    document.get(key) match {
      case value: Integer =>
        value.toDouble
      case value: String =>
        value.toDouble
      case value: java.lang.Long =>
        value.toDouble
      case value: java.lang.Double =>
        value
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        0.0
    }
  }

  def getIntString(document: Document, key: String): String = {
    document.get(key) match {
      case value: Integer =>
        value + ""
      case value: String =>
        value
      case value: java.lang.Long =>
        value + ""
      case value: java.lang.Double =>
        val str = String.valueOf(value)
        str.substring(0, str.indexOf("."))
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        "0"
    }
  }

  def getInt(document: Document, key: String): Int = {
    document.get(key) match {
      case value: Integer =>
        value
      case value: String =>
        value.toInt
      case value: java.lang.Long =>
        value.toInt
      case value: java.lang.Double =>
        value.toInt
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        -1
    }
  }

  def getLongString(document: Document, key: String): String = {
    document.get(key) match {
      case value: Integer =>
        value + ""
      case value: String =>
        value
      case value: java.lang.Long =>
        value + ""
      case value: java.lang.Double =>
        val str = String.valueOf(value)
        str.substring(0, str.indexOf("."))
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        "0"
    }
  }

  def getLong(document: Document, key: String): Long = {
    document.get(key) match {
      case value: Integer =>
        value.toLong
      case value: String =>
        value.toLong
      case value: java.lang.Long =>
        value.toLong
      case value: java.lang.Double =>
        value.toLong
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        0
    }
  }


  def getTimestamp(document: Document, key: String, formatString: String = "yyyy-MM-dd HH:mm:ss"): Int = {
    document.get(key) match {
      case value: Integer =>
        value
      case value: java.lang.Long =>
        if (value > Integer.MAX_VALUE) {
          (value / 1000).toInt
        } else {
          value.toInt
        }
      case value: String =>
        if (isNumber(value)) {
          if (value.length == 10)
            value.toInt
          else
            (value.toLong / 1000).toInt
        } else {
          TimeUtil.dateString2Time(formatString, value, TimeUtil.SECOND).toInt
        }
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        0
    }
  }

  def getDocument(document: Document, key: String): Document = {
    document.get(key) match {
      case value: Document =>
        value
      case null =>
        new Document()
      case _ =>
        new Document().append(key, document.get(key))
    }
  }

  def getSecondColumnString(document: Document, first_key: String, second_key: String): String = {
    getString(getDocument(document, first_key), second_key)
  }

  def getSecondColumnDouble(document: Document, first_key: String, second_key: String): Double = {
    getDouble(getDocument(document, first_key), second_key)
  }

  def getSecondColumnInteger(document: Document, first_key: String, second_key: String): Int = {
    getInt(getDocument(document, first_key), second_key)
  }

  def getSecondColumnLong(document: Document, first_key: String, second_key: String): Long = {
    getLong(getDocument(document, first_key), second_key)
  }

  def isNumber(string: String): Boolean = {
    var flag = true
    for (i <- string.indices) {
      if (string.charAt(i) < '0' || string.charAt(i) > '9') {
        flag = false
      }
    }
    flag
  }

  def agentOldLog2Bson(log: String): (Document, Int, String) = {

    //NORMAL: [2018-01-08 12:11:04] CIP[119.54.199.236] CONTENT[{"user_id":"1000920627","channel_id":"10169","game_id":"136","channel_name":"h5agent","game_channel_id":""}]

    val firstStr = "NORMAL: ["
    val secondStr = "] CIP["
    val thirdStr = "CONTENT["

    val json_str = log.substring(log.indexOf(thirdStr) + thirdStr.length, log.length - 1)
    val time = log.substring(log.indexOf(firstStr) + firstStr.length, log.indexOf(secondStr))
    val ip = log.substring(log.indexOf(secondStr) + secondStr.length, log.indexOf(thirdStr) - 2)
    val tmpDoc = Document.parse(json_str)
    (tmpDoc, TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", time, TimeUtil.SECOND).toInt, ip)
  }

  def log2bson(log: String): (Document, Boolean) = { //包括AF日志 AD_AF_LOG[
    val startStr = "DATA["
    if (log.contains(startStr)) {
      val json_str = log.substring(log.indexOf(startStr) + startStr.length, log.length - 1)
      (Document.parse(json_str), true)
    } else {
      (new Document().append("AD_AF_LOG", log), false)
    }
  }


  def BsonParser(log: String, thirdStr: String): (Document, Int, String) = {
    val firstStr = "NORMAL: ["
    val secondStr = "] CIP["
    // NORMAL: [2018-03-02 19:12:30] CIP[182.84.195.55] ACTIVE_LOG
    // [
    //  {
    //   "extra_data":
    //     {"screen_size":"960|540","device_id":"00000000-3be3-e5e3-dfc7-96034d1a4a02","device_name":"RwoZI nPuU617",
    //     "imsi":"865166027042699","imei":"865166027042699","system_version":"5.1.1","sdk_version":"2.1",
    //     "package_name":"com.junhai.shj2.dalan","application_name":"山海经2","application_version":181},
    //   "app_data":
    //     {"agent_game_id":"146","game_channel_id":"103278","action_type":"start","time":"1519989148"}
    //  }
    // ]

    val activeLog = log.substring(log.indexOf(thirdStr) + thirdStr.length, log.length - 1)
    val time = log.substring(log.indexOf(firstStr) + firstStr.length, log.indexOf(secondStr))
    val ip = log.substring(log.indexOf(secondStr) + secondStr.length, log.indexOf(thirdStr) - 2)
    (Document.parse(activeLog), TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", time, TimeUtil.SECOND).toInt, ip)
  }

  def toDocument(junhaiLog: JunhaiLog): Document = {
    val document = new Document()

    val user_part = new Document()
    user_part.put(user_id, JunhaiLog.user_id)
    user_part.put(user_name, JunhaiLog.user_name)
    user_part.put(gender, JunhaiLog.gender)

    val game_part = new Document()
    val agent_part = new Document()
    val device_part = new Document()
    val role_part = new Document()
    val mission_part = new Document()
    val copy_part = new Document()
    val trade_part = new Document()
    val order_part = new Document()
    val click_part = new Document()
    val custom_part = new Document()
    val channel_platform_part = new Document()
    val junhai_sdk_part = new Document()

    document.put(user, user_part)
    document.put(game, game_part)
    document.put(agent, agent_part)
    document.put(device, device_part)
    document.put(role, role_part)
    document.put(mission, mission_part)
    document.put(copy, copy_part)
    document.put(trade, trade_part)
    document.put(order, order_part)
    document.put(click, click_part)
    document.put(custom, custom_part)
    document.put(channel_platform, channel_platform_part)
    document.put(junhai_sdk, junhai_sdk_part)
    document
  }

  def getFlatOrder(junhaiLog: JunhaiLog): Document = {
    val document = new Document()
    document.put(JunhaiLog.user_id, junhaiLog.user_id)
    document.put(JunhaiLog.user_name, junhaiLog.user_name)
    document.put(JunhaiLog.gender, junhaiLog.gender)
    document.put(JunhaiLog.birth, junhaiLog.birth)
    document.put(JunhaiLog.age, junhaiLog.age)
    document.put(JunhaiLog.open_id, junhaiLog.open_id)
    document.put(JunhaiLog.phone_num, junhaiLog.phone_num)
    document.put(JunhaiLog.email_num, junhaiLog.email_num)
    document.put(JunhaiLog.login_type, junhaiLog.login_type)

    document.put(JunhaiLog.game_id, junhaiLog.game_id)
    document.put(JunhaiLog.game_name, junhaiLog.game_name)
    document.put(JunhaiLog.game_ver, junhaiLog.game_ver)

    document.put(JunhaiLog.channel_id, junhaiLog.channel_id)
    document.put(JunhaiLog.game_channel_id, junhaiLog.game_channel_id)
    document.put(JunhaiLog.access_token, junhaiLog.access_token)

    document.put(JunhaiLog.screen_height, junhaiLog.screen_height)
    document.put(JunhaiLog.screen_width, junhaiLog.screen_width)
    document.put(JunhaiLog.device_id, junhaiLog.device_id)
    document.put(JunhaiLog.ios_idfa, junhaiLog.ios_idfa)
    document.put(JunhaiLog.android_imei, junhaiLog.android_imei)
    document.put(JunhaiLog.android_adv_id, junhaiLog.android_adv_id)
    document.put(JunhaiLog.android_id, junhaiLog.android_id)
    document.put(JunhaiLog.device_name, junhaiLog.device_name)
    document.put(JunhaiLog.os_ver, junhaiLog.os_ver)
    document.put(JunhaiLog.os_type, junhaiLog.os_type)
    document.put(JunhaiLog.sdk_ver, junhaiLog.sdk_ver)
    document.put(JunhaiLog.package_name, junhaiLog.package_name)
    document.put(JunhaiLog.net_type, junhaiLog.net_type)
    document.put(JunhaiLog.user_agent, junhaiLog.user_agent)

    document.put(JunhaiLog.role_level, junhaiLog.role_level)
    document.put(JunhaiLog.role_name, junhaiLog.role_name)
    document.put(JunhaiLog.role_server, junhaiLog.role_server)
    document.put(JunhaiLog.role_id, junhaiLog.role_id)
    document.put(JunhaiLog.role_type, junhaiLog.role_type)
    document.put(JunhaiLog.role_gender, junhaiLog.role_gender)
    document.put(JunhaiLog.association_id, junhaiLog.association_id)
    document.put(JunhaiLog.association_name, junhaiLog.association_name)
    document.put(JunhaiLog.association_rank, junhaiLog.association_rank)

    document.put(JunhaiLog.order_sn, junhaiLog.order_sn)
    document.put(JunhaiLog.cp_trade_sn, junhaiLog.cp_trade_sn)
    document.put(JunhaiLog.channel_trade_sn, junhaiLog.channel_trade_sn)
    document.put(JunhaiLog.request_url, junhaiLog.request_url)
    document.put(JunhaiLog.request_result, junhaiLog.request_result)
    document.put(JunhaiLog.http_code, junhaiLog.http_code)
    document.put(JunhaiLog.cny_amount, junhaiLog.cny_amount)
    document.put(JunhaiLog.currency_amount, junhaiLog.currency_amount)
    document.put(JunhaiLog.money, junhaiLog.currency_amount) //dalan数据合到独代
    document.put(JunhaiLog.currency_type, junhaiLog.currency_type)
    document.put(JunhaiLog.usd_amount, junhaiLog.usd_amount)
    document.put(JunhaiLog.order_type, junhaiLog.order_type)
    document.put(JunhaiLog.pay_status, junhaiLog.pay_status)
    document.put(JunhaiLog.status, junhaiLog.status)


    document.put(JunhaiLog.event, junhaiLog.event)
    document.put(JunhaiLog.is_test, junhaiLog.is_test)
    document.put(JunhaiLog.data_ver, junhaiLog.data_ver)
    document.put(JunhaiLog.client_time_zone, junhaiLog.client_time_zone)
    document.put(JunhaiLog.client_ts, junhaiLog.client_ts)
    document.put(JunhaiLog.server_time_zone, junhaiLog.server_time_zone)
    document.put(JunhaiLog.server_ts, junhaiLog.server_ts)
    document.put(JunhaiLog.client_ip, junhaiLog.client_ip)
    document
  }


  def getOrder(junhaiLog: JunhaiLog): Document = {
    val document = new Document()
    val user = new Document()
    val game = new Document()
    val agent = new Document()
    val device = new Document()
    val role = new Document()
    val order = new Document()

    user.put(JunhaiLog.user_id, junhaiLog.user_id)
    user.put(JunhaiLog.user_name, junhaiLog.user_name)
    user.put(JunhaiLog.gender, junhaiLog.gender)
    user.put(JunhaiLog.birth, junhaiLog.birth)
    user.put(JunhaiLog.age, junhaiLog.age)
    user.put(JunhaiLog.open_id, junhaiLog.open_id)
    user.put(JunhaiLog.phone_num, junhaiLog.phone_num)
    user.put(JunhaiLog.email_num, junhaiLog.email_num)
    user.put(JunhaiLog.login_type, junhaiLog.login_type)

    game.put(JunhaiLog.game_id, junhaiLog.game_id)
    game.put(JunhaiLog.game_name, junhaiLog.game_name)
    game.put(JunhaiLog.game_ver, junhaiLog.game_ver)

    agent.put(JunhaiLog.channel_id, junhaiLog.channel_id)
    agent.put(JunhaiLog.game_channel_id, junhaiLog.game_channel_id)
    agent.put(JunhaiLog.access_token, junhaiLog.access_token)

    device.put(JunhaiLog.screen_height, junhaiLog.screen_height)
    device.put(JunhaiLog.screen_width, junhaiLog.screen_width)
    device.put(JunhaiLog.device_id, junhaiLog.device_id)
    device.put(JunhaiLog.ios_idfa, junhaiLog.ios_idfa)
    device.put(JunhaiLog.android_imei, junhaiLog.android_imei)
    device.put(JunhaiLog.android_adv_id, junhaiLog.android_adv_id)
    device.put(JunhaiLog.android_id, junhaiLog.android_id)
    device.put(JunhaiLog.device_name, junhaiLog.device_name)
    device.put(JunhaiLog.os_ver, junhaiLog.os_ver)
    device.put(JunhaiLog.os_type, junhaiLog.os_type)
    device.put(JunhaiLog.sdk_ver, junhaiLog.sdk_ver)
    device.put(JunhaiLog.package_name, junhaiLog.package_name)
    device.put(JunhaiLog.net_type, junhaiLog.net_type)
    device.put(JunhaiLog.user_agent, junhaiLog.user_agent)

    role.put(JunhaiLog.role_level, junhaiLog.role_level)
    role.put(JunhaiLog.role_name, junhaiLog.role_name)
    role.put(JunhaiLog.role_server, junhaiLog.role_server)
    role.put(JunhaiLog.role_id, junhaiLog.role_id)
    role.put(JunhaiLog.role_type, junhaiLog.role_type)
    role.put(JunhaiLog.role_gender, junhaiLog.role_gender)
    role.put(JunhaiLog.association_id, junhaiLog.association_id)
    role.put(JunhaiLog.association_name, junhaiLog.association_name)
    role.put(JunhaiLog.association_rank, junhaiLog.association_rank)

    order.put(JunhaiLog.order_sn, junhaiLog.order_sn)
    order.put(JunhaiLog.cp_trade_sn, junhaiLog.cp_trade_sn)
    order.put(JunhaiLog.channel_trade_sn, junhaiLog.channel_trade_sn)
    order.put(JunhaiLog.request_url, junhaiLog.request_url)
    order.put(JunhaiLog.request_result, junhaiLog.request_result)
    order.put(JunhaiLog.http_code, junhaiLog.http_code)
    order.put(JunhaiLog.order_step, junhaiLog.order_step)
    order.put(JunhaiLog.cny_amount, junhaiLog.cny_amount) //haiwai金额字段
    order.put(JunhaiLog.money, junhaiLog.currency_amount) //dalan数据合到独代
    order.put(JunhaiLog.currency_type, junhaiLog.currency_type)
    order.put(JunhaiLog.usd_amount, junhaiLog.usd_amount)
    order.put(JunhaiLog.order_status, junhaiLog.order_status)
    order.put(JunhaiLog.order_type, junhaiLog.order_type)

    document.put(JunhaiLog.user, user)
    document.put(JunhaiLog.game, game)
    document.put(JunhaiLog.agent, agent)
    document.put(JunhaiLog.device, device)
    document.put(JunhaiLog.role, role)
    document.put(JunhaiLog.order, order)

    document.put(JunhaiLog.event, junhaiLog.event)
    document.put(JunhaiLog.is_test, junhaiLog.is_test)
    document.put(JunhaiLog.data_ver, junhaiLog.data_ver)
    document.put(JunhaiLog.client_time_zone, junhaiLog.client_time_zone)
    document.put(JunhaiLog.client_ts, junhaiLog.client_ts)
    document.put(JunhaiLog.server_time_zone, junhaiLog.server_time_zone)
    document.put(JunhaiLog.server_ts, junhaiLog.server_ts)
    document.put(JunhaiLog.client_ip, junhaiLog.client_ip)
    document
  }

  def getRegister(junhaiLog: JunhaiLog): Document = {
    val document = new Document()
    val user = new Document()
    val game = new Document()
    val device = new Document()
    val agent = new Document()

    user.put(JunhaiLog.user_id, junhaiLog.user_id)
    user.put(JunhaiLog.user_name, junhaiLog.user_name)
    user.put(JunhaiLog.gender, junhaiLog.gender)
    user.put(JunhaiLog.birth, junhaiLog.birth)
    user.put(JunhaiLog.age, junhaiLog.age)
    user.put(JunhaiLog.open_id, junhaiLog.open_id)
    user.put(JunhaiLog.phone_num, junhaiLog.phone_num)
    user.put(JunhaiLog.email_num, junhaiLog.email_num)
    user.put(JunhaiLog.login_type, junhaiLog.login_type)

    game.put(JunhaiLog.game_id, junhaiLog.game_id)
    game.put(JunhaiLog.game_name, junhaiLog.game_name)
    game.put(JunhaiLog.game_ver, junhaiLog.game_ver)

    agent.put(JunhaiLog.channel_id, junhaiLog.channel_id)
    agent.put(JunhaiLog.game_channel_id, junhaiLog.game_channel_id)
    agent.put(JunhaiLog.access_token, junhaiLog.access_token)

    device.put(JunhaiLog.screen_height, junhaiLog.screen_height)
    device.put(JunhaiLog.screen_width, junhaiLog.screen_width)
    device.put(JunhaiLog.device_id, junhaiLog.device_id)
    device.put(JunhaiLog.ios_idfa, junhaiLog.ios_idfa)
    device.put(JunhaiLog.android_imei, junhaiLog.android_imei)
    device.put(JunhaiLog.android_adv_id, junhaiLog.android_adv_id)
    device.put(JunhaiLog.android_id, junhaiLog.android_id)
    device.put(JunhaiLog.device_name, junhaiLog.device_name)
    device.put(JunhaiLog.os_ver, junhaiLog.os_ver)
    device.put(JunhaiLog.os_type, junhaiLog.os_type)
    device.put(JunhaiLog.sdk_ver, junhaiLog.sdk_ver)
    device.put(JunhaiLog.package_name, junhaiLog.package_name)
    device.put(JunhaiLog.net_type, junhaiLog.net_type)
    device.put(JunhaiLog.user_agent, junhaiLog.user_agent)

    document.put(JunhaiLog.status, junhaiLog.status)
    document.put(JunhaiLog.event, junhaiLog.event)
    document.put(JunhaiLog.is_test, junhaiLog.is_test)
    document.put(JunhaiLog.data_ver, junhaiLog.data_ver)
    document.put(JunhaiLog.client_time_zone, junhaiLog.client_time_zone)
    document.put(JunhaiLog.client_ts, junhaiLog.client_ts)
    document.put(JunhaiLog.server_time_zone, junhaiLog.server_time_zone)
    document.put(JunhaiLog.server_ts, junhaiLog.server_ts)
    document.put(JunhaiLog.client_ip, junhaiLog.client_ip)

    document.put(JunhaiLog.user, user)
    document.put(JunhaiLog.game, game)
    document.put(JunhaiLog.device, device)
    document.put(JunhaiLog.agent, agent)
    document
  }

  /**
    * 装载数据
    *
    * @param document
    * @return
    */
  def load(document: Document): JunhaiLog = {
    val junhaiLog = new JunhaiLog
    junhaiLog.access_token = getSecondColumnString(document, agent, access_token)
    junhaiLog.channel_id = getSecondColumnString(document, agent, channel_id)
    junhaiLog.game_channel_id = getSecondColumnString(document, agent, game_channel_id)

    junhaiLog.user_id = getSecondColumnString(document, user, user_id)
    junhaiLog.user_agent = getSecondColumnString(document, user, user_agent)
    junhaiLog.user_name = getSecondColumnString(document, user, user_name)
    junhaiLog.open_id = getSecondColumnString(document, user, open_id)
    junhaiLog.gender = getSecondColumnString(document, user, gender)
    junhaiLog.birth = getSecondColumnString(document, user, birth)
    junhaiLog.age = getSecondColumnString(document, user, age)
    junhaiLog.phone_num = getSecondColumnString(document, user, phone_num)
    junhaiLog.email_num = getSecondColumnString(document, user, email_num)
    junhaiLog.login_type = getSecondColumnString(document, user, login_type)

    junhaiLog.game_id = getSecondColumnString(document, game, game_id)
    junhaiLog.game_name = getSecondColumnString(document, game, game_name)
    junhaiLog.game_ver = getSecondColumnString(document, game, game_ver)

    junhaiLog.device_name = getSecondColumnString(document, device, device_name)
    junhaiLog.screen_height = getSecondColumnString(document, device, screen_height)
    junhaiLog.screen_width = getSecondColumnString(document, device, screen_width)
    junhaiLog.device_id = getSecondColumnString(document, device, device_id)
    junhaiLog.ios_idfa = getSecondColumnString(document, device, ios_idfa)
    junhaiLog.android_imei = getSecondColumnString(document, device, android_imei)
    junhaiLog.android_adv_id = getSecondColumnString(document, device, android_adv_id)
    junhaiLog.android_id = getSecondColumnString(document, device, android_id)
    junhaiLog.device_name = getSecondColumnString(document, device, device_name)
    junhaiLog.os_ver = getSecondColumnString(document, device, os_ver)
    junhaiLog.sdk_ver = getSecondColumnString(document, device, sdk_ver)
    junhaiLog.package_name = getSecondColumnString(document, device, package_name)
    junhaiLog.os_type = getSecondColumnString(document, device, os_type)
    junhaiLog.net_type = getSecondColumnString(document, device, net_type)
    junhaiLog.user_agent = getSecondColumnString(document, device, user_agent)

    junhaiLog.role_level = getSecondColumnString(document, role, role_level)
    junhaiLog.role_name = getSecondColumnString(document, role, role_name)
    junhaiLog.role_server = getSecondColumnString(document, role, role_server)
    junhaiLog.role_id = getSecondColumnString(document, role, role_id)
    junhaiLog.role_type = getSecondColumnString(document, role, role_type)
    junhaiLog.role_gender = getSecondColumnString(document, role, role_gender)
    junhaiLog.association_id = getSecondColumnString(document, role, association_id)
    junhaiLog.association_name = getSecondColumnString(document, role, association_name)
    junhaiLog.association_rank = getSecondColumnString(document, role, association_rank)

    junhaiLog.mission_id = getSecondColumnString(document, mission, mission_id)
    junhaiLog.mission_name = getSecondColumnString(document, mission, mission_name)
    junhaiLog.mission_type = getSecondColumnString(document, mission, mission_type)
    junhaiLog.mission_process = getSecondColumnString(document, mission, mission_process)
    junhaiLog.mission_step = getSecondColumnString(document, mission, mission_step)

    junhaiLog.copy_id = getSecondColumnString(document, copy, copy_id)
    junhaiLog.copy_name = getSecondColumnString(document, copy, copy_name)
    junhaiLog.copy_level = getSecondColumnString(document, copy, copy_level)
    junhaiLog.copy_diffculty = getSecondColumnString(document, copy, copy_diffculty)
    junhaiLog.copy_process = getSecondColumnString(document, copy, copy_process)

    junhaiLog.trade_type = getSecondColumnString(document, trade, trade_type)
    junhaiLog.trade_amount = getSecondColumnString(document, trade, trade_amount)
    junhaiLog.remain_amount = getSecondColumnString(document, trade, remain_amount)
    junhaiLog.item_name = getSecondColumnString(document, trade, item_name)
    junhaiLog.item_amount = getSecondColumnString(document, trade, item_amount)
    junhaiLog.trade_id = getSecondColumnString(document, trade, trade_id)
    junhaiLog.trade_desc = getSecondColumnString(document, trade, trade_desc)
    junhaiLog.is_bind = getSecondColumnString(document, trade, is_bind)

    junhaiLog.order_sn = getSecondColumnString(document, order, order_sn)
    junhaiLog.cp_trade_sn = getSecondColumnString(document, order, cp_trade_sn)
    junhaiLog.channel_trade_sn = getSecondColumnString(document, order, channel_trade_sn)
    junhaiLog.request_url = getSecondColumnString(document, order, request_url)
    junhaiLog.http_code = getSecondColumnString(document, order, http_code)
    junhaiLog.request_result = getSecondColumnString(document, order, request_result)
    junhaiLog.order_step = getSecondColumnString(document, order, order_step)
    junhaiLog.currency_amount = getSecondColumnDouble(document, order, currency_amount)
    junhaiLog.currency_type = getSecondColumnString(document, order, currency_type)
    junhaiLog.order_status = getSecondColumnString(document, order, order_status)
    junhaiLog.order_type = getSecondColumnString(document, order, order_type)

    junhaiLog.ad_id = getSecondColumnString(document, channel_platform, ad_id)
    junhaiLog.app_id = getSecondColumnString(document, junhai_sdk, app_id)

    junhaiLog.status = getString(document, status)
    junhaiLog.event = getString(document, event)
    junhaiLog.is_test = getString(document, is_test)
    junhaiLog.data_ver = getString(document, data_ver)
    junhaiLog.client_time_zone = getString(document, client_time_zone)
    junhaiLog.client_ts = getInt(document, client_ts)
    junhaiLog.server_time_zone = getString(document, server_time_zone)
    junhaiLog.server_ts = getInt(document, server_ts)
    junhaiLog.client_ip = getString(document, client_ip)
    junhaiLog
  }

  /** 一层清洗
    * sql.Date 在Document中不能识别，所以需要清洗
    *
    * @param document
    * @return
    */
  def documentClean2Json(document: Document): Document = {
    val keys = document.keySet()
    for (key <- keys) {
      document.get(key) match {
        case value: java.sql.Date =>
          document.put(key, TimeUtil.time2DateString("yyyy-MM-dd HH:mm:ss", value.getTime, TimeUtil.MILLISECOND))
        case _ =>

      }
    }
    document
  }

  def getBoolean(document: Document, key: String): Boolean = {
    document.get(key) match {
      case value: Integer =>
        false
      case value: String =>
        false
      case value: java.lang.Long =>
        false
      case value: java.lang.Double =>
        false
      case value: java.lang.Boolean =>
        value
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        false
    }
  }

  /** 一层清洗
    * 需要将字符串的时间转为sql.Date
    *
    * @param document
    * @return
    */
  def documentClean2GP(document: Document, sqlDateKeys: Array[String]): Document = {
    sqlDateKeys.foreach(key => {
      val timeString = JunhaiLog.getString(document, key)
      if (timeString.length > 13)
        document.put(key, TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss", JunhaiLog.getString(document, key)))
      else
        document.put(key, TimeUtil.dataString2SqlDate("yyyy-MM-dd", JunhaiLog.getString(document, key)))
    })
    document
  }


}
