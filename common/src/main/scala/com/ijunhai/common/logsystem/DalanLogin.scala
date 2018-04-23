package com.ijunhai.common.logsystem

import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by Admin on 2017-11-09.
  */
class DalanLogin {
  var login_time = TimeUtil.getDefaultSqlDate
  var id = ""
  var login_date = 0
  var user_id = ""
  var user_name = ""
  var game = ""
  var channel = ""
  var device_id = ""
  var login_ip = ""
  var pf = 0

   def getkey(junhaidate:Int): String = {
    id + junhaidate + login_time + login_date + user_id + user_name + game + channel + device_id + login_ip + pf
  }
}

object DalanLogin {
  val login_time = "login_time"
  val id = "id"
  val login_date = "login_date"
  val user_id = "user_id"
  val user_name = "user_name"
  val game = "game"
  val channel = "channel"
  val device_id = "device_id"
  val login_ip = "login_ip"
  val pf = "pf"

  //  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  //  `login_date` int(11) NOT NULL DEFAULT '19700701' COMMENT '登录日期',
  //  `user_id` int(11) NOT NULL DEFAULT '0' COMMENT '用户ID',
  //  `user_name` varchar(50) NOT NULL DEFAULT '' COMMENT '用户名',
  //  `game` varchar(50) NOT NULL DEFAULT '' COMMENT '游戏名',
  //  `channel` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道名',
  //  `device_id` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
  //  `login_ip` varchar(20) NOT NULL DEFAULT '0.0.0.0' COMMENT '用户ID',
  //  `login_time` datetime NOT NULL DEFAULT '1970-07-01 00:00:00' COMMENT '登录时间',
  //  `pf` tinyint(2) NOT NULL DEFAULT '0' COMMENT '0安卓1IOS2H5',
  //  +----+------------+---------+-------------+-----------+----------+--------------------------------------+--------------+---------------------+----+
  //  | id | login_date | user_id | user_name   | game      | channel  | device_id                            | login_ip     | login_time          | pf |
  //  +----+------------+---------+-------------+-----------+----------+--------------------------------------+--------------+---------------------+----+
  //  |  1 |   20161028 |    3887 | 13995260505 | 100000125 | appstore | 86E5C262-20DB-42F7-A71F-7EF5683D0EBB | 123.59.86.88 | 2016-10-28 00:51:03 |  1 |
  //  +----+------------+---------+-------------+-----------+----------+--------------------------------------+--------------+---------------------+----+
  def load(document: Document): DalanLogin = {
    val dalanLogin = new DalanLogin
    val junhaiLog = JunhaiLog.load(document)
    dalanLogin.login_date = TimeUtil.time2DateString("yyyyMMdd", junhaiLog.server_ts, TimeUtil.SECOND).toInt
    dalanLogin.login_time = TimeUtil.time2SqlDate(junhaiLog.server_ts, TimeUtil.SECOND)
    dalanLogin.user_id = junhaiLog.user_id
    dalanLogin.user_name = junhaiLog.user_name
    dalanLogin.game = junhaiLog.game_id
    dalanLogin.channel = junhaiLog.ad_id
    dalanLogin.device_id = junhaiLog.device_id
    dalanLogin.login_ip = junhaiLog.client_ip
    dalanLogin.pf = if (dalanLogin.channel == "appstore") 2 else 0
    dalanLogin.id = MD5Util.getMD5(dalanLogin.getkey(junhaiLog.server_ts))
    dalanLogin
  }

  def unload(dalanLogin: DalanLogin): Document = {
    val document = new Document()
    document.put(login_date, dalanLogin.login_date)
    document.put(user_id, dalanLogin.user_id)
    document.put(user_name, dalanLogin.user_name)
    document.put(game, dalanLogin.game)
    document.put(channel, dalanLogin.channel)
    document.put(device_id, dalanLogin.device_id)
    document.put(login_ip, dalanLogin.login_ip)
    document.put(login_time, dalanLogin.login_time)
    document.put(pf, dalanLogin.pf)
    document.put(id, dalanLogin.id)
    document
  }

}
