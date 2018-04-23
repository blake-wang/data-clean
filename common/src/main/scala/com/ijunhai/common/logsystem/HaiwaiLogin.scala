package com.ijunhai.common.logsystem

import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by Admin on 2018-01-31.
  */
class HaiwaiLogin {
  var login_time=TimeUtil.getDefaultSqlDate
  var id = ""
  var login_date = 0
  var user_id = ""
  var user_name = ""
  var game = ""
  var channel = ""
  var device_id = ""
  var login_ip = ""
  var pf = 0
  var imei = ""
  def getkey(junhaidate:Int): String = {
    id + junhaidate + login_time + login_date + user_id + user_name + game + channel + device_id + login_ip + pf
  }
}

object HaiwaiLogin {

//  id         | character varying(50)       | not null
//  login_date | integer                     | not null default 19700701
//  user_id    | character varying(100)      | not null default '0'::character varying
//  user_name  | character varying(50)       | not null default ''::character varying
//  game       | character varying(50)       | not null default ''::character varying
//  channel    | character varying(50)       | not null default ''::character varying
//  device_id  | character varying(50)       | not null default ''::character varying
//  login_ip   | character varying(20)       | not null default '0.0.0.0'::character varying
//  login_time | timestamp without time zone | not null default '1970-07-01 00:00:00'::timestamp without time zone
//  pf         | smallint                    | not null default 0::smallint
//  imei       | character varying(50)       | not null default ''::character varying
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
  val imei="imei"
  def load(document: Document): HaiwaiLogin = {
    val haiwaiLogin = new HaiwaiLogin
    val junhaiLog = JunhaiLog.load(document)
    haiwaiLogin.login_date = TimeUtil.time2DateString("yyyyMMdd", junhaiLog.server_ts, TimeUtil.SECOND).toInt
    haiwaiLogin.login_time = TimeUtil.time2SqlDate(junhaiLog.server_ts, TimeUtil.SECOND)
    haiwaiLogin.user_id = junhaiLog.user_id
    haiwaiLogin.user_name = junhaiLog.user_name
    haiwaiLogin.game = junhaiLog.game_id
    haiwaiLogin.channel = junhaiLog.ad_id
    haiwaiLogin.device_id = junhaiLog.device_id
    haiwaiLogin.login_ip = junhaiLog.client_ip
    haiwaiLogin.pf = if (haiwaiLogin.channel == "appstore") 2 else 0
    haiwaiLogin.id = MD5Util.getMD5(haiwaiLogin.getkey(junhaiLog.server_ts))
    haiwaiLogin.imei=junhaiLog.imei
    haiwaiLogin
  }
  def unload(haiwaiLogin: HaiwaiLogin): Document = {
    val document = new Document()
    document.put(login_date, haiwaiLogin.login_date)
    document.put(user_id, haiwaiLogin.user_id)
    document.put(user_name, haiwaiLogin.user_name)
    document.put(game, haiwaiLogin.game)
    document.put(channel, haiwaiLogin.channel)
    document.put(device_id, haiwaiLogin.device_id)
    document.put(login_ip, haiwaiLogin.login_ip)
    document.put(login_time, haiwaiLogin.login_time)
    document.put(pf, haiwaiLogin.pf)
    document.put(id, haiwaiLogin.id)
    document.put(imei,haiwaiLogin.imei)
    document
  }

}
