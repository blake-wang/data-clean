package com.ijunhai.common.logsystem

import com.ijunhai.common.{MD5Util, TimeUtil}
import org.bson.Document

/**
  * Created by Admin on 2018-03-07.
  */
class HwVoidOrder {
  var id=""
  var create_time=TimeUtil.getDefaultSqlDate
  var purchase_date=0
  var voided_date=0
  var mDeveloperPayload=""
  var mItemType=""
  var mOrderId=""
  var orderid=""
  var packageName=""
  var productId=""
  var purchase_time=TimeUtil.getDefaultSqlDate
  var voided_time=TimeUtil.getDefaultSqlDate
  var purchaseState=0
  var developerPayload=""
  var purchaseToken=""
  var mPurchaseTime=TimeUtil.getDefaultSqlDate
  var order_id=""
  var amount=0.0
  var jh_app_id=""
  var mark=0              //是否退款标志 0 没有退款  1 退款
  var game_id=""
  var pack_name=""
  override def toString: String ={
    //    order_sn+user_id+money+type_ +local_money+imf_money+status+pay_status+game+big_channel+channel+device_id+server+pay_type+cp_trade_sn+channel_trade_sn+goods_id+goods_name+game_role_id+game_role_name+game_url+create_time+update_time+platform+pf
    purchaseToken+order_id+mPurchaseTime.getTime
  }
}

object HwVoidOrder{
  val id="id"
  val create_time="create_time"
  val purchase_time="purchase_time"
  val voided_time="voided_time"
  val purchase_date="purchase_date"
  val voided_date="voided_date"
  val mDeveloperPayload="mDeveloperPayload"
  val mItemType="mItemType"
  val mOrderId="mOrderId"
  val mOriginalJson="mOriginalJson"
  val orderId="orderId"
  val packageName="packageName"
  val productId="productId"
  val purchaseTime="purchaseTime"
  val purchaseState="purchaseState"
  val developerPayload="developerPayload"
  val purchaseToken="purchaseToken"
  val mPackageName="mPackageName"
  val mPurchaseState="mPurchaseState"
  val mPurchaseTime="mPurchaseTime"
  val mSignature="mSignature"
  val mSku="mSku"
  val mToken="mToken"
  val amount="amount"
  val order_id="order_id"
  val jh_app_id="jh_app_id"
  val mark="mark"
  val event_order="event_order"
  val pack_name="pack_name"
  val product_id="product_id"
  val purchase_token="purchase_token"
  val purchase_state="purchase_state"
  val order_sn="order_sn"
  val game_id="game_id"

  val google_lf="google_lf"
  //获取token
  val grant_type="grant_type"
  val client_id="client_id"
  val client_secret="client_secret"
  val refresh_token="refresh_token"

  val grant_type_value="refresh_token"
  val client_id_value="599268142253-hieftkej3o5ej6ikrv595vu66ngapidm.apps.googleusercontent.com"
  val client_secret_value="gLWZYIwOzZxmvyEDUl40v7bP"
  val refresh_token_value="1/bH0WknnYvhGNSO5INFie4jtZY07cErtRgNzjIj0l7hSB1aFtGeZ4ggtmZsk-clKe"

  def load(document: Document): HwVoidOrder ={
    val hwVoidOrder=new HwVoidOrder
    hwVoidOrder.mPurchaseTime=TimeUtil.time2SqlDate(JunhaiLog.getLong(document,HwVoidOrder.mPurchaseTime),TimeUtil.MILLISECOND)
    hwVoidOrder.purchase_date=TimeUtil.time2DateString("yyyyMMdd",JunhaiLog.getLong(document,HwVoidOrder.mPurchaseTime),TimeUtil.MILLISECOND).toInt
   // hwVoidOrder.voided_time=if(JunhaiLog.getString(document,HwVoidOrder.voided_time)=="") TimeUtil.getDefaultSqlDate else TimeUtil.time2SqlDate(JunhaiLog.getInt(document,HwVoidOrder.voided_time),TimeUtil.SECOND)
    //hwVoidOrder.voided_date=if(JunhaiLog.getString(document,HwVoidOrder.voided_time)=="") 0 else TimeUtil.time2DateString("yyyyMMdd",JunhaiLog.getInt(document,HwVoidOrder.voided_time),TimeUtil.SECOND).toInt
    hwVoidOrder.mDeveloperPayload=JunhaiLog.getString(document,HwVoidOrder.mDeveloperPayload)
    hwVoidOrder.mItemType=JunhaiLog.getString(document,HwVoidOrder.mItemType)
    hwVoidOrder.mOrderId=JunhaiLog.getString(document,HwVoidOrder.mOrderId)
    hwVoidOrder.packageName=JunhaiLog.getSecondColumnString(document,HwVoidOrder.mOriginalJson,HwVoidOrder.packageName)
    hwVoidOrder.productId=JunhaiLog.getSecondColumnString(document,HwVoidOrder.mOriginalJson,HwVoidOrder.productId)
    hwVoidOrder.purchaseState=JunhaiLog.getSecondColumnLong(document,HwVoidOrder.mOriginalJson,HwVoidOrder.mPurchaseState).toInt
    hwVoidOrder.purchaseToken=JunhaiLog.getString(document,HwVoidOrder.mToken)
    hwVoidOrder.game_id=JunhaiLog.getString(document,HwVoidOrder.jh_app_id)
    hwVoidOrder.mark=JunhaiLog.getInt(document,HwVoidOrder.mark)
    hwVoidOrder.order_id=JunhaiLog.getString(document,HwVoidOrder.order_id)
    hwVoidOrder.amount=JunhaiLog.getDouble(document,HwVoidOrder.amount)/100
    hwVoidOrder.id=MD5Util.getMD5(hwVoidOrder.toString())
    hwVoidOrder

  }
  def unload(hwVoidOrder: HwVoidOrder): Document = {
    val document = new Document()
    document.put(id,hwVoidOrder.id)
    document.put(purchase_time,hwVoidOrder.mPurchaseTime)
   // document.put(voided_time,hwVoidOrder.voided_time)
    document.put(purchase_date,hwVoidOrder.purchase_date)
    //document.put(voided_date,hwVoidOrder.voided_date)
    document.put(mDeveloperPayload,hwVoidOrder.mDeveloperPayload)
    document.put(mItemType,hwVoidOrder.mItemType)
    document.put(order_id,hwVoidOrder.mOrderId)
    document.put(pack_name,hwVoidOrder.packageName)
    document.put(product_id,hwVoidOrder.productId)
    document.put(purchase_state,hwVoidOrder.purchaseState)
    document.put(purchase_token,hwVoidOrder.purchaseToken)
    document.put(game_id,hwVoidOrder.game_id)
    document.put(amount,hwVoidOrder.amount)
    document.put(order_sn,hwVoidOrder.order_id)

    document

  }

}
