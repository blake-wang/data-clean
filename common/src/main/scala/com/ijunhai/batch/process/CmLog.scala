package com.ijunhai.batch.process

import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by admin on 2018/2/22.
  */
  class CmOrder{
    var id=""
    var order_sn=""
    var user_id=""
    var money=0.0
    var type_ =""
    var local_money=0.0
    var imf_money=0.0
    var status=0
    var pay_status=0
    var game=""
    var big_channel=0
    var channel=""
    var device_id=""
    var server=0
    var pay_type=""
    var cp_trade_sn=""
    var channel_trade_sn=""
    var goods_id=""
    var goods_name=""
    var game_role_id=""
    var game_role_name=""
    var game_url=""
    var create_time=TimeUtil.getDefaultSqlDate
    var update_time=TimeUtil.getDefaultSqlDate
    var platform=0
    var pf=0
    var imei= ""
    var order_date = 0
    var create_ip = ""
    override def toString(): String ={
      //    order_sn+user_id+money+type_ +local_money+imf_money+status+pay_status+game+big_channel+channel+device_id+server+pay_type+cp_trade_sn+channel_trade_sn+goods_id+goods_name+game_role_id+game_role_name+game_url+create_time+update_time+platform+pf
      order_sn+status+pay_status
    }
  }
  object CmOrder {
    val id="id"
    val order_sn="order_sn"
    val user_id="user_id"
    val money="money"
    val type_ ="type"
    val local_money="local_money"
    val imf_money="imf_money"
    val status="status"
    val pay_status="pay_status"
    val game="game"
    val big_channel="big_channel"
    val channel="channel"
    val device_id="device_id"
    val server="server"
    val pay_type="pay_type"
    val cp_trade_sn="cp_trade_sn"
    val channel_trade_sn="channel_trade_sn"
    val goods_id="goods_id"
    val goods_name="goods_name"
    val game_role_id="game_role_id"
    val game_role_name="game_role_name"
    val game_url="game_url"
    val create_time="create_time"
    val update_time="update_time"
    val platform="platform"
    val pf="pf"
    val imei= "imei"
    val order_date = "order_date"
    val create_ip = "create_ip"
    //  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    //  `order_sn` varchar(50) NOT NULL DEFAULT '0' COMMENT '订单序列号',
    //  `user_id` varchar(100) NOT NULL DEFAULT '' COMMENT '用户ID',
    //  `money` decimal(20,4) NOT NULL DEFAULT '0.0000' COMMENT '付费金额',
    //  `type` varchar(20) NOT NULL DEFAULT '' COMMENT '货币类型',
    //  `local_money` decimal(20,4) DEFAULT '0.0000' COMMENT '当地货币金额',
    //  `imf_money` decimal(20,4) DEFAULT '0.0000' COMMENT '国际货币',
    //  `status` tinyint(2) NOT NULL DEFAULT '1' COMMENT '付费流程状态1已下单2已付费3通知CP失败4通知CP成功5CP处理完成',
    //  `pay_status` tinyint(2) NOT NULL DEFAULT '0' COMMENT '付费状态0失败1成功',
    //  `game` varchar(50) NOT NULL DEFAULT '' COMMENT '付费游戏',
    //  `big_channel` int(11) NOT NULL DEFAULT '0' COMMENT '大渠道',
    //  `channel` varchar(50) NOT NULL DEFAULT '' COMMENT '付费所属渠道',
    //  `device_id` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
    //  `server` int(11) NOT NULL DEFAULT '0' COMMENT '游戏服',
    //  `pay_type` varchar(50) NOT NULL DEFAULT '' COMMENT '支付渠道1支付宝5易宝6微信SDK版9网页支付宝10手机WAP版11爱贝支付12微信扫码13微信手机网页版',
    //  `cp_trade_sn` varchar(50) NOT NULL DEFAULT '' COMMENT 'CP交易号',
    //  `channel_trade_sn` varchar(50) DEFAULT '' COMMENT '渠道交易号',
    //  `goods_id` varchar(20) NOT NULL DEFAULT '' COMMENT '物品ID',
    //  `goods_name` varchar(50) DEFAULT '' COMMENT '物品名',
    //  `game_role_id` varchar(20) NOT NULL DEFAULT '' COMMENT '游戏角色ID',
    //  `game_role_name` varchar(50) NOT NULL DEFAULT '' COMMENT '游戏角色名称',
    //  `game_url` varchar(255) DEFAULT '' COMMENT '调用游戏方兑换地址',
    //  `create_time` datetime NOT NULL COMMENT '订单创建时间',
    //  `update_time` datetime NOT NULL DEFAULT '1970-07-01 00:00:00' COMMENT '订单更新时间',
    //  `platform` tinyint(2) NOT NULL DEFAULT '0' COMMENT '1君海2独代3海外4大蓝5冰鸟6神奇',
    //  `pf` tinyint(1) NOT NULL DEFAULT '0' COMMENT '0安卓1IOS2H5',
    //  +----+---------------------+---------+--------+------+-------------+-----------+--------+------------+---------+-------------+---------+--------------------------------------+--------+----------+---------------------+------------------+----------+------------+--------------+----------------+------------------------------------------------------+---------------------+---------------------+----------+----+
    //  | id | order_sn            | user_id | money  | type | local_money | imf_money | status | pay_status | game    | big_channel | channel | device_id                            | server | pay_type | cp_trade_sn         | channel_trade_sn | goods_id | goods_name | game_role_id | game_role_name | game_url                                             | create_time         | update_time         | platform | pf |
    //  +----+---------------------+---------+--------+------+-------------+-----------+--------+------------+---------+-------------+---------+--------------------------------------+--------+----------+---------------------+------------------+----------+------------+--------------+----------------+------------------------------------------------------+---------------------+---------------------+----------+----+
    //  |  1 | 2017012224529541618 | 64755   | 6.0000 | 1    |      0.0000 |    0.0000 |      1 |          0 | sqbblbs |          18 | lianyun | 00000000-0469-ace5-60ae-1eea0033c587 |   6001 | 6        | 2016091317341150846 |                  | 60钻石   | 60钻石     | 64755        | 一个人         | http://agent.ijunhai.com/pay/payFinish/channel_id/18 | 2017-01-22 10:56:36 | 2017-01-22 10:56:36 |        1 |  0 |
    //  +----+---------------------+---------+--------+------+-------------+-----------+--------+------------+---------+-------------+---------+--------------------------------------+--------+----------+---------------------+------------------+----------+------------+--------------+----------------+------------------------------------------------------+---------------------+---------------------+----------+----+
    def load(document: Document): CmOrder ={
      val cmOrder=new CmOrder
      cmOrder.order_sn=JunhaiLog.getString(document,CmOrder.order_sn)
      cmOrder.user_id=JunhaiLog.getString(document,CmOrder.user_id)
      cmOrder.money=JunhaiLog.getDouble(document,CmOrder.money)
      cmOrder.type_ =JunhaiLog.getString(document,CmOrder.type_)
      cmOrder.local_money=JunhaiLog.getDouble(document,CmOrder.local_money)
      cmOrder.imf_money=JunhaiLog.getDouble(document,CmOrder.imf_money)

      cmOrder.pay_status=JunhaiLog.getInt(document,CmOrder.pay_status)
      cmOrder.status= if (cmOrder.pay_status ==1) 2 else JunhaiLog.getInt(document,CmOrder.status) //批处理要把status设为2
      cmOrder.game=JunhaiLog.getString(document,"game_id")
      cmOrder.big_channel=JunhaiLog.getInt(document,CmOrder.big_channel)
      cmOrder.channel=JunhaiLog.getString(document,"jh_channel")
      cmOrder.device_id=JunhaiLog.getString(document,CmOrder.device_id)
      cmOrder.server=JunhaiLog.getInt(document,CmOrder.server)
      cmOrder.pay_type=JunhaiLog.getString(document,CmOrder.pay_type)
      cmOrder.cp_trade_sn=JunhaiLog.getString(document,CmOrder.cp_trade_sn)
      cmOrder.channel_trade_sn=JunhaiLog.getString(document,CmOrder.channel_trade_sn)
      cmOrder.goods_id=JunhaiLog.getString(document,CmOrder.goods_id)
      cmOrder.goods_name=JunhaiLog.getString(document,CmOrder.goods_name)
      cmOrder.game_role_id=JunhaiLog.getString(document,CmOrder.game_role_id)
      cmOrder.game_role_name=JunhaiLog.getString(document,CmOrder.game_role_name)
      cmOrder.game_url=JunhaiLog.getString(document,CmOrder.game_url)
      cmOrder.create_time=TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,CmOrder.create_time))
      cmOrder.update_time=TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document,CmOrder.update_time))
      cmOrder.platform=4
      cmOrder.pf=if(cmOrder.channel=="appstore") 2 else 0
      cmOrder.id=MD5Util.getMD5(cmOrder.toString())
      cmOrder.imei = JunhaiLog.getString(document, CmOrder.imei)
      cmOrder.order_date = TimeUtil.time2DateString("yyyyMMdd", TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss",JunhaiLog.getString(document, CmOrder.update_time), TimeUtil.SECOND), TimeUtil.SECOND).toInt
      cmOrder.create_ip = JunhaiLog.getString(document, CmOrder.create_ip)

      cmOrder
    }
    def unload(cmOrder: CmOrder): Document ={
      val document=new Document()
      document.put(order_sn,cmOrder.order_sn)
      document.put(user_id,cmOrder.user_id)
      document.put(money,cmOrder.money)
      document.put(type_,cmOrder.type_)
      document.put(local_money,cmOrder.local_money)
      document.put(imf_money,cmOrder.imf_money)
      document.put(status,cmOrder.status)
      document.put(pay_status,cmOrder.pay_status)
      document.put(game,cmOrder.game)
      document.put(big_channel,cmOrder.big_channel)
      document.put(channel,cmOrder.channel)
      document.put(device_id,cmOrder.device_id)
      document.put(server,cmOrder.server)
      document.put(pay_type,cmOrder.pay_type)
      document.put(cp_trade_sn,cmOrder.cp_trade_sn)
      document.put(channel_trade_sn,cmOrder.channel_trade_sn)
      document.put(goods_id,cmOrder.goods_id)
      document.put(goods_name,cmOrder.goods_name)
      document.put(game_role_id,cmOrder.game_role_id)
      document.put(game_role_name,cmOrder.game_role_name)
      document.put(game_url,cmOrder.game_url)
      document.put(create_time,cmOrder.create_time)
      document.put(update_time,cmOrder.update_time)
      document.put(platform,cmOrder.platform)
      document.put(pf,cmOrder.pf)
      document.put(id,cmOrder.id)
      document.put(imei,cmOrder.imei)
      document.put(order_date,cmOrder.order_date) //int
      document.put(create_ip,cmOrder.create_ip)

      document
    }

}
