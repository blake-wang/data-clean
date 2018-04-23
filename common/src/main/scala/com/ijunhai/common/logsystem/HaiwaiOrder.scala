package com.ijunhai.common.logsystem

import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by Admin on 2018-01-30.
  */
class HaiwaiOrder{
  var id=""
  var order_sn=""
  var user_id=""
  var money=0.0
  var money_type=""
  var type_ =""
  var local_money=0.0
  var imf_money=0.0
  var status=0
  var pay_status=0
  var game=""
  var big_channel=0
  var channel=""
  var device_id =""
  var server =0
  var pay_type =""
  var cp_trade_sn=""
  var channel_trade_sn=""
  var goods_id =""
  var goods_name=""
  var game_role_id =""
  var game_role_name=""
  var game_url=""
  var create_time=TimeUtil.getDefaultSqlDate
  var update_time=TimeUtil.getDefaultSqlDate
  var pf  =0
  var imei =""
  var order_date =0
  var  create_ip  =""

  override def toString: String ={
    //    order_sn+user_id+money+type_ +local_money+imf_money+status+pay_status+game+big_channel+channel+device_id+server+pay_type+cp_trade_sn+channel_trade_sn+goods_id+goods_name+game_role_id+game_role_name+game_url+create_time+update_time+platform+pf
    order_sn+status+pay_status+create_time.getTime+update_time.getTime
  }
}

object HaiwaiOrder {
  val id = "id"
  val order_sn = "order_sn"
  val user_id = "user_id"
  val money ="money"  //人民币
  var money_type="money_type"
  val type_ ="type"
  val local_money = "local_money"//当地货币
  val imf_money = "imf_money" //美元
  val status = "status"
  val pay_status = "pay_status"
  val game = "game"           //game_id
  val big_channel = "big_channel"
  val channel = "channel"
  val device_id = "device_id"
  val server="server"
  val pay_type = "pay_type"
  val cp_trade_sn = "cp_trade_sn"
  val channel_trade_sn = "channel_trade_sn"
  val payment_trade_sn="payment_trade_sn"
  val goods_id = "goods_id"
  val goods_name = "goods_name"
  val game_role_id ="game_role_id"
  val game_role_name = "game_role_name"
  val game_url = "game_url"
  val create_time = "create_time"
  val update_time = "update_time"
  val pf = "pf"   //空 人为去转
  val imei = ""     //空
  val order_date = "order_date"
  val create_ip = "create_ip" //空
  val rmb="rmb"
  val usd="usd"

  //  id               | character varying(50)       | not null
  //  order_sn         | character varying(50)       | not null default '0'::character varying
  //  user_id          | character varying(100)      | not null default ''::character varying
  //  money            | numeric(20,4)               | not null default 0.0000
  //  type             | character varying(20)       | not null default ''::character varying
  //  local_money      | numeric(20,4)               | default 0.0000
  //  imf_money        | numeric(20,4)               | default 0.0000
  //  status           | smallint                    | not null default 1::smallint
  //    pay_status       | smallint                    | not null default 0::smallint
  //    game             | character varying(50)       | not null default ''::character varying
  //  big_channel      | integer                     | not null default 0
  //  channel          | character varying(50)       | not null default ''::character varying
  //  device_id        | character varying(50)       | not null default ''::character varying
  //  server           | integer                     | not null default 0
  //  pay_type         | character varying(50)       | not null default ''::character varying
  //  cp_trade_sn      | character varying(50)       | not null default ''::character varying
  //  channel_trade_sn | character varying(50)       | default ''::character varying
  //  goods_id         | character varying(100)      | not null default ''::character varying
  //  goods_name       | character varying(50)       | default ''::character varying
  //  game_role_id     | character varying(100)      | not null default ''::character varying
  //  game_role_name   | character varying(50)       | not null default ''::character varying
  //  game_url         | character varying(255)      | default ''::character varying
  //  create_time      | timestamp without time zone | not null default '1970-07-01 00:00:00'::timestamp without time zone
  //  update_time      | timestamp without time zone | not null default '1970-07-01 00:00:00'::timestamp without time zone
  //  pf               | smallint                    | not null default 0::smallint
  //  imei             | character varying(50)       | not null default ''::character varying
  //  order_date       | integer                     | not null default 0
  //  create_ip        | character varying(20)       | not null default '127.0.0.1'::character varying
  def load(document: Document): HaiwaiOrder = {
    val haiwaiOrder=new HaiwaiOrder
    haiwaiOrder.order_sn=JunhaiLog.getString(document,HaiwaiOrder.order_sn)
    haiwaiOrder.user_id=JunhaiLog.getString(document,HaiwaiOrder.user_id)
    haiwaiOrder.money_type=JunhaiLog.getString(document,HaiwaiOrder.money_type)//哪种货币
    haiwaiOrder.money=JunhaiLog.getDouble(document,HaiwaiOrder.rmb)//代表的是人民币
    haiwaiOrder.type_ =JunhaiLog.getString(document,HaiwaiOrder.type_)
    haiwaiOrder.local_money=JunhaiLog.getDouble(document,HaiwaiOrder.money)//当地货币  topic存的是money
    haiwaiOrder.imf_money=JunhaiLog.getDouble(document,HaiwaiOrder.usd)//代表的是usb
    haiwaiOrder.status=JunhaiLog.getInt(document,HaiwaiOrder.status)
    haiwaiOrder.pay_status=JunhaiLog.getInt(document,HaiwaiOrder.pay_status)
    haiwaiOrder.game=JunhaiLog.getString(document,"game_id")
    //haiwaiOrder.big_channel=JunhaiLog.getInt(document,HaiwaiOrder.big_channel)
    haiwaiOrder.channel=JunhaiLog.getString(document,"jh_channel")
    haiwaiOrder.device_id=JunhaiLog.getString(document,HaiwaiOrder.device_id)
    haiwaiOrder.server=JunhaiLog.getInt(document,HaiwaiOrder.server)
    haiwaiOrder.pay_type=JunhaiLog.getString(document,HaiwaiOrder.pay_type)
    haiwaiOrder.cp_trade_sn=JunhaiLog.getString(document,HaiwaiOrder.cp_trade_sn)
    haiwaiOrder.channel_trade_sn=JunhaiLog.getString(document,HaiwaiOrder.payment_trade_sn)
    haiwaiOrder.goods_id=JunhaiLog.getString(document,HaiwaiOrder.goods_id)
    haiwaiOrder.goods_name=JunhaiLog.getString(document,HaiwaiOrder.goods_name)
    haiwaiOrder.game_role_id=JunhaiLog.getString(document,HaiwaiOrder.game_role_id)
    haiwaiOrder.game_role_name=JunhaiLog.getString(document,HaiwaiOrder.game_role_name)
    haiwaiOrder.game_url=JunhaiLog.getString(document,HaiwaiOrder.game_url)
    haiwaiOrder.create_time=TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,HaiwaiOrder.create_time))
    haiwaiOrder.update_time=TimeUtil.time2SqlDate(JunhaiLog.getLong(document,HaiwaiOrder.update_time),TimeUtil.MILLISECOND)
    haiwaiOrder.pf=if(HaiwaiOrder.channel=="appstore") 2 else 0
    haiwaiOrder.order_date=TimeUtil.time2DateString("yyyyMMdd",TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,HaiwaiOrder.create_time),TimeUtil.SECOND),TimeUtil.SECOND).toInt
    haiwaiOrder.id=MD5Util.getMD5(haiwaiOrder.toString())



    haiwaiOrder

  }
  def unload(haiwaiOrder: HaiwaiOrder): Document ={

    val document=new Document()
    document.put(order_sn,haiwaiOrder.order_sn)
    document.put(user_id,haiwaiOrder.user_id)
    document.put(money,haiwaiOrder.money)
    document.put(type_,haiwaiOrder.type_)
    document.put(local_money,haiwaiOrder.local_money)
    document.put(imf_money,haiwaiOrder.imf_money)
    document.put(status,haiwaiOrder.status)
    document.put(pay_status,haiwaiOrder.pay_status)
    document.put(game,haiwaiOrder.game)
    document.put(big_channel,haiwaiOrder.big_channel)
    document.put(channel,haiwaiOrder.channel)
    document.put(device_id,haiwaiOrder.device_id)
    //document.put(server,haiwaiOrder.server)
    document.put(pay_type,haiwaiOrder.pay_type)
    document.put(cp_trade_sn,haiwaiOrder.cp_trade_sn)
    document.put(channel_trade_sn,haiwaiOrder.channel_trade_sn)
    document.put(goods_id,haiwaiOrder.goods_id)
    document.put(goods_name,haiwaiOrder.goods_name)
    document.put(game_role_id,haiwaiOrder.game_role_id)
    document.put(game_role_name,haiwaiOrder.game_role_name)
    document.put(game_url,haiwaiOrder.game_url)
    document.put(create_time,haiwaiOrder.create_time)
    document.put(update_time,haiwaiOrder.update_time)
    document.put(pf,haiwaiOrder.pf)
    document.put(id,haiwaiOrder.id)
    document.put(order_date,haiwaiOrder.order_date)
    document.put(money_type,haiwaiOrder.money_type)
    document.put(server,haiwaiOrder.server)

    document
  }
}
