package com.ijunhai.common.logsystem

import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by Admin on 2017-11-09.
  */
class DalanUser {
  var id = ""
  var user_id = "" //int
  var user_name = ""
  var tel_num = ""
  var tel_status = 0
  var email = ""
  var reg_type = 0 //int
  var channel = ""
  var game = ""
  var device_id = ""
  var register_ip = ""
  var register_time = TimeUtil.getDefaultSqlDate
  var status = 0 //int
  var platform = 0 //int
  var pf = 0 //int
  var new_device = 0 //int
  var imei = ""
  var reg_date = 0

  override def toString: String = {
    user_id
  }
}

object DalanUser {
  val id = "id"
  val user_id = "user_id" //int
  val user_name = "user_name"
  val tel_num = "tel_num"
  val tel_status = "tel_status"
  val email = "email"
  val reg_type = "reg_type" //int
  val channel = "channel"
  val game = "game"
  val device_id = "device_id"
  val register_ip = "register_ip"
  val register_time = "register_time"
  val status = "status" //int
  val platform = "platform" //int
  val pf = "pf" //int
  val new_device = "new_device" //int
  val imei = "imei"
  val reg_date = "reg_date"
  //  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  //  `user_id` int(11) NOT NULL COMMENT '用户ID',
  //  `user_name` varchar(30) NOT NULL DEFAULT '' COMMENT '账号',
  //  `tel_num` varchar(20) DEFAULT '' COMMENT '手机号码',
  //  `tel_status` tinyint(2) NOT NULL DEFAULT '0' COMMENT '手机号码验证状态0未验证1已验证',
  //  `email` varchar(256) DEFAULT '' COMMENT '邮箱',
  //  `reg_type` tinyint(2) NOT NULL DEFAULT '1' COMMENT '注册类型1手机注册2邮箱注册3自由注册4一键注册',
  //  `channel` varchar(50) NOT NULL DEFAULT '' COMMENT '用户来源渠道',
  //  `game` varchar(50) NOT NULL DEFAULT '' COMMENT '用户注册应用名'(已经转成game_id字段),
  //  `device_id` varchar(255) NOT NULL DEFAULT '' COMMENT '设备ID',
  //  `register_ip` varchar(20) NOT NULL DEFAULT '127.0.0.1' COMMENT '注册IP',
  //  `register_time` datetime NOT NULL DEFAULT '1970-07-01 00:00:00' COMMENT '注册时间',
  //  `status` tinyint(2) NOT NULL DEFAULT '1' COMMENT '用户账号状态1正常0废弃',
  //  `platform` tinyint(2) NOT NULL DEFAULT '1' COMMENT '1国内3海外4大蓝5冰鸟6神奇',
  //  `pf` tinyint(2) NOT NULL DEFAULT '0' COMMENT '0安卓1IOS2H5',
  //  `new_device` tinyint(2) NOT NULL DEFAULT '1' COMMENT '0否1是',
  //  +---------+---------+------------+---------+------------+-------+----------+---------+-------+-----------+-------------+---------------------+--------+----------+----+------------+
  //  | id      | user_id | user_name  | tel_num | tel_status | email | reg_type | channel | game  | device_id | register_ip | register_time       | status | platform | pf | new_device |
  //  +---------+---------+------------+---------+------------+-------+----------+---------+-------+-----------+-------------+---------------------+--------+----------+----+------------+
  //  | 1242592 |       2 | 1000000001 |         |          0 |       |        4 | ijunhai | nzsgz |           | 127.0.0.1   | 2014-10-17 18:24:03 |      1 |        1 |  0 |          1 |
  //  +---------+---------+------------+---------+------------+-------+----------+---------+-------+-----------+-------------+---------------------+--------+----------+----+------------+
  //  id    | user_id | user_name  | tel_num | tel_status | email | reg_type | channel     |   game | device_id|register_ip  | register_time    | status | platform | pf | new_device    | imei | reg_date

  def load(document: Document): DalanUser = {
    val dalanUser = new DalanUser
    dalanUser.user_id = JunhaiLog.getString(document, DalanUser.user_id)
    dalanUser.user_name = JunhaiLog.getString(document, DalanUser.user_name)
    dalanUser.tel_num = JunhaiLog.getString(document, DalanUser.tel_num)
    dalanUser.tel_status = JunhaiLog.getInt(document, DalanUser.tel_status)
    dalanUser.email = JunhaiLog.getString(document, DalanUser.email)
    dalanUser.reg_type = JunhaiLog.getInt(document, DalanUser.reg_type)
    dalanUser.channel = JunhaiLog.getString(document, "jh_channel")
    dalanUser.game = JunhaiLog.getString(document, "jh_app_id")
    dalanUser.device_id = JunhaiLog.getString(document, DalanUser.device_id)
    dalanUser.register_ip = JunhaiLog.getString(document, DalanUser.register_ip)
    dalanUser.register_time = TimeUtil.time2SqlDate(JunhaiLog.getInt(document, DalanUser.register_time), TimeUtil.SECOND)
    dalanUser.status = JunhaiLog.getInt(document, DalanUser.status)
    dalanUser.platform = 4
    //    youyun_ad库里面表的pf字段，如果union_channel或者channel值为appstore，刚pf=2,否则为0
    dalanUser.pf = if (dalanUser.channel == "appstore") 2 else 0
    dalanUser.new_device = JunhaiLog.getInt(document, DalanUser.new_device)
    dalanUser.id = MD5Util.getMD5(dalanUser.toString())
    dalanUser.imei = JunhaiLog.getString(document, DalanUser.imei)
    dalanUser.reg_date = TimeUtil.time2DateString("yyyyMMdd", JunhaiLog.getInt(document, DalanUser.register_time), TimeUtil.SECOND).toInt
    dalanUser
  }

  def unload(dalanUser: DalanUser): Document = {
    val document = new Document()
    document.put(user_id, dalanUser.user_id) //int
    document.put(user_name, dalanUser.user_name)
    document.put(tel_num, dalanUser.tel_num)
    document.put(tel_status, dalanUser.tel_status)
    document.put(email, dalanUser.email)
    document.put(reg_type, dalanUser.reg_type) //int
    document.put(channel, dalanUser.channel)
    document.put(game, dalanUser.game)
    document.put(device_id, dalanUser.device_id)
    document.put(register_ip, dalanUser.register_ip)
    document.put(register_time, dalanUser.register_time)
    document.put(status, dalanUser.status) //int
    document.put(platform, dalanUser.platform) //int
    document.put(pf, dalanUser.pf) //int
    document.put(new_device, dalanUser.new_device) //int
    document.put(id, dalanUser.id)
    document.put(imei,dalanUser.imei)
    document.put(reg_date,dalanUser.reg_date) //int
    document
  }
}
