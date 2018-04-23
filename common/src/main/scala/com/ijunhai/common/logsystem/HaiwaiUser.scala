package com.ijunhai.common.logsystem

import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by Admin on 2018-01-30.
  */
class  HaiwaiUser{
  var id=""
  var user_id=""
  var user_name=""
  var tel_num=""
  var tel_status=0
  var email=""
  var reg_type =1
  var channel=""
  var game=""
  var device_id=""
  var imei =""
  var reg_date =0
  var  register_ip=""
  var register_time=TimeUtil.getDefaultSqlDate
  var status=1
  var  pf =0
  var  new_device =0
  var  jh_app_id=""
  override def toString: String ={
    id+user_id+user_name+tel_num+tel_status+email+reg_type+channel+game+device_id+register_ip+register_time+status+pf+new_device
  }
}

object HaiwaiUser {
  val id="id"
  val user_id="user_id"
  val user_name="user_name"
  val tel_num="tel_num"
  val tel_status="tel_status"
  val email="email"
  val reg_type ="reg_type"
  val channel="channel"
  val game="game"          //
  val device_id="device_id"
  val imei ="imei "  //
  val reg_date ="reg_date "
  val  register_ip="register_ip"
  val register_time="register_time"
  val status="status"
  val  pf =" pf "
  val  new_device ="new_device"//
  val jh_app_id="jh_app_id"

//  id            | character varying(50)       | not null
//  user_id       | character varying(100)      | not null
//  user_name     | character varying(30)       | not null default ''::character varying
//  tel_num       | character varying(20)       | default ''::character varying
//  tel_status    | smallint                    | not null default 0::smallint
//  email         | character varying(256)      | default ''::character varying
//  reg_type      | smallint                    | not null default 1::smallint
//  channel       | character varying(50)       | not null default ''::character varying
//  game          | character varying(50)       | not null default ''::character varying
//  device_id     | character varying(255)      | not null default ''::character varying
//  imei          | character varying(50)       | not null default ''::character varying
//  reg_date      | integer                     | not null default 0
//  register_ip   | character varying(20)       | not null default '127.0.0.1'::character varying
//  register_time | timestamp without time zone | not null default '1970-07-01 00:00:00'::timestamp without time zone
//  status        | smallint                    | not null default 1::smallint
//    pf            | smallint                    | not null default 0::smallint
//    new_device    | smallint                    | not null default 1::smallint
def load(document: Document): HaiwaiUser ={
  val haiwaiUser=new HaiwaiUser
  haiwaiUser.user_id=JunhaiLog.getString(document,HaiwaiUser.user_id)
  haiwaiUser.user_name=JunhaiLog.getString(document,HaiwaiUser.user_name)
  haiwaiUser.tel_num=JunhaiLog.getString(document,HaiwaiUser.tel_num)
  haiwaiUser.tel_status=JunhaiLog.getInt(document,HaiwaiUser.tel_status)
  haiwaiUser.email=JunhaiLog.getString(document,HaiwaiUser.email)
  haiwaiUser.reg_type=JunhaiLog.getInt(document,HaiwaiUser.reg_type)
  haiwaiUser.channel=JunhaiLog.getString(document,"jh_channel")
  haiwaiUser.game=JunhaiLog.getString(document,HaiwaiUser.jh_app_id)
  haiwaiUser.device_id=JunhaiLog.getString(document,HaiwaiUser.device_id)
  haiwaiUser.imei=JunhaiLog.getString(document,HaiwaiUser.imei)
  haiwaiUser.register_ip=JunhaiLog.getString(document,HaiwaiUser.register_ip)
  haiwaiUser.reg_date=TimeUtil.time2DateString("yyyyMMdd",JunhaiLog.getInt(document,register_time),TimeUtil.SECOND).toInt
  haiwaiUser.register_time=TimeUtil.time2SqlDate(JunhaiLog.getInt(document,DalanUser.register_time),TimeUtil.SECOND)
  haiwaiUser.status=JunhaiLog.getInt(document,HaiwaiUser.status)
  haiwaiUser.pf=if (haiwaiUser.channel=="appstore") 2 else 0
  haiwaiUser.new_device=0
  haiwaiUser.id=MD5Util.getMD5(haiwaiUser.toString())
  haiwaiUser
}
  def unload(haiwaiUser: HaiwaiUser): Document ={
    val document=new Document()
    document.put(user_id,haiwaiUser.user_id)//int
    document.put(user_name,haiwaiUser.user_name)
    document.put(tel_num,haiwaiUser.tel_num)
    document.put(tel_status,haiwaiUser.tel_status)
    document.put(email,haiwaiUser.email)
    document.put(reg_type,haiwaiUser.reg_type)//int
    document.put(channel,haiwaiUser.channel)
    document.put(game,haiwaiUser.game)
    document.put(device_id,haiwaiUser.device_id)
    document.put(register_ip,haiwaiUser.register_ip)
    document.put(register_time,haiwaiUser.register_time)
    document.put(status,haiwaiUser.status)//int
    document.put(pf,haiwaiUser.pf)//int
    document.put(new_device,haiwaiUser.new_device)//int
    document.put(id,haiwaiUser.id)
    document.put(imei,haiwaiUser.imei)
    document.put(reg_date,haiwaiUser.reg_date)
    document

  }

}
