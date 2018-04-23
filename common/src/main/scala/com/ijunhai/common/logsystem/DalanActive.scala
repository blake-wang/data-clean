package com.ijunhai.common.logsystem

import java.sql.Date

import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by Admin on 2017-11-09.
  */
class DalanActive {
  var id = ""
  var active_date = 0
  var union_app_id = 0
  var union_channel = ""
  var action_type = ""
  var client_time = TimeUtil.getDefaultSqlDate
  var screen_size = ""
  var device_id = ""
  var device_name = ""
  var imsi = ""
  var imei = ""
  var system_version = ""
  var sdk_version = ""
  var package_name = ""
  var application_name = ""
  var application_version = ""
  var create_time = TimeUtil.getDefaultSqlDate
  var server_ip = ""

  def getkey(): String = {
    union_app_id + device_id
  }

//  def getkey(junhaidate: Int): String = {
//    union_app_id + junhaidate + device_id + create_time
//  }
}

object DalanActive {
  val id = "id"
  val create_time = "create_time"
  val active_date = "active_date"
  val union_app_id = "union_app_id"
  val union_channel = "union_channel"
  val action_type = "action_type"
  val client_time = "client_time"
  val screen_size = "screen_size"
  val device_id = "device_id"
  val device_name = "device_name"
  val imsi = "imsi"
  val imei = "imei"
  val system_version = "system_version"
  val sdk_version = "sdk_version"
  val package_name = "package_name"
  val application_name = "application_name"
  val application_version = "application_version"
  val server_ip = "server_ip"
  //  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  //  `active_date` int(11) NOT NULL DEFAULT '0' COMMENT '激活日期',
  //  `union_app_id` int(11) NOT NULL DEFAULT '0' COMMENT '大蓝游戏唯一标识',
  //  `union_channel` varchar(255) NOT NULL DEFAULT '0' COMMENT '渠道标识',
  //  `action_type` varchar(255) NOT NULL DEFAULT '' COMMENT '事件类型',
  //  `client_time` datetime NOT NULL DEFAULT '1970-07-01 00:00:00' COMMENT '客户端时间',
  //  `screen_size` varchar(20) NOT NULL DEFAULT '' COMMENT '屏幕分辨率',
  //  `device_id` varchar(50) NOT NULL DEFAULT '' COMMENT '设备唯一标识',
  //  `device_name` varchar(50) NOT NULL DEFAULT '' COMMENT '设备名',
  //  `imsi` varchar(50) NOT NULL DEFAULT '' COMMENT 'IMSI号',
  //  `imei` varchar(50) NOT NULL DEFAULT '' COMMENT 'IMEI号',
  //  `system_version` varchar(20) NOT NULL DEFAULT '' COMMENT '系统版本',
  //  `sdk_version` varchar(20) NOT NULL DEFAULT '' COMMENT 'SDK版本',
  //  `package_name` varchar(50) NOT NULL DEFAULT '' COMMENT '包名',
  //  `application_name` varchar(50) NOT NULL DEFAULT '' COMMENT '应用名',
  //  `application_version` varchar(20) NOT NULL DEFAULT '' COMMENT '应用版本',
  //  `create_time` datetime NOT NULL DEFAULT '1970-07-01 00:00:00' COMMENT '请求时间',
  //  `server_ip` varchar(20) NOT NULL DEFAULT '127.0.0.1' COMMENT '客户端IP',

  //  +----+-------------+--------------+-------------------------+-------------+---------------------+-------------+--------------------------------------+-------------+-----------------+-----------------+----------------+-------------+------------------------+------------------+---------------------+---------------------+----------------+
  //  | id | active_date | union_app_id | union_channel           | action_type | client_time         | screen_size | device_id                            | device_name | imsi            | imei            | system_version | sdk_version | package_name           | application_name | application_version | create_time         | server_ip      |
  //  +----+-------------+--------------+-------------------------+-------------+---------------------+-------------+--------------------------------------+-------------+-----------------+-----------------+----------------+-------------+------------------------+------------------+---------------------+---------------------+----------------+
  //  |  1 |    20170223 |    100000209 | WM_bd_sbkryol_ad_177_37 | start       | 2017-02-23 17:47:31 | 1920|1080   | ffffffff-c4cb-4fe6-ffff-ffffaebb8bab | Vivo X7     | 460023689036588 | 863102035060961 | 5.1.1          | 4.0         | com.junhai.sbkry.dalan | 沙巴克荣耀       | 100                 | 2017-02-23 17:47:31 | 112.17.246.211 |
  //  +----+-------------+--------------+-------------------------+-------------+---------------------+-------------+--------------------------------------+-------------+-----------------+-----------------+----------------+-------------+------------------------+------------------+---------------------+---------------------+----------------+
  def load(document: Document): DalanActive = {
    val dalanActive = new DalanActive
    val junhaiLog = JunhaiLog.load(document)
    dalanActive.active_date = TimeUtil.time2DateString("yyyyMMdd", junhaiLog.server_ts, TimeUtil.SECOND).toInt
    dalanActive.create_time = TimeUtil.time2SqlDate(junhaiLog.server_ts, TimeUtil.SECOND)
    dalanActive.server_ip = junhaiLog.client_ip
    dalanActive.union_app_id = if (junhaiLog.game_id != "") junhaiLog.game_id.toInt else -1
    dalanActive.union_channel = junhaiLog.ad_id
    dalanActive.action_type = ""
    dalanActive.client_time = TimeUtil.time2SqlDate(junhaiLog.server_ts, TimeUtil.SECOND)
    dalanActive.screen_size = junhaiLog.screen_height + "|" + junhaiLog.screen_width
    dalanActive.device_id = junhaiLog.device_id
    dalanActive.device_name = junhaiLog.device_name
    dalanActive.imsi = ""
    dalanActive.imei = junhaiLog.android_imei
    dalanActive.system_version = junhaiLog.os_ver
    dalanActive.sdk_version = junhaiLog.sdk_ver
    dalanActive.package_name = junhaiLog.package_name
    dalanActive.application_name = junhaiLog.game_name
    dalanActive.application_version = junhaiLog.game_ver
    dalanActive.id = MD5Util.getMD5(dalanActive.getkey())
    dalanActive
  }

  def unload(dalanActive: DalanActive): Document = {
    val document = new Document()
    document.put(active_date, dalanActive.active_date)
    document.put(union_app_id, dalanActive.union_app_id)
    document.put(union_channel, dalanActive.union_channel)
    document.put(action_type, dalanActive.action_type)
    document.put(client_time, dalanActive.client_time)
    document.put(screen_size, dalanActive.screen_size)
    document.put(device_id, dalanActive.device_id)
    document.put(device_name, dalanActive.device_name)
    document.put(imsi, dalanActive.imsi)
    document.put(imei, dalanActive.imei)
    document.put(system_version, dalanActive.system_version)
    document.put(sdk_version, dalanActive.sdk_version)
    document.put(package_name, dalanActive.package_name)
    document.put(application_name, dalanActive.application_name)
    document.put(application_version, dalanActive.application_version)
    document.put(create_time, dalanActive.create_time)
    document.put(server_ip, dalanActive.server_ip)
    document.put(id, dalanActive.id)
    document
  }

}
