package com.ijunhai.process.agent

import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.storage.greenplum.Save2GP
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

/** 渠道数据去关联字段，并作汇率转换操作
  * Created by Admin on 2017-10-19.
  */
object ChannelTransform {
  val error = "error"
  val FORMAT_STR = "yyyy-MM-dd HH:mm:ss"
  val HWGameMap = Map("hw_csgj" -> "100000000", "hw_hwdzz" -> "100000001", "hw_djcz" -> "100000002",
    "hw_csgjhwb" -> "100000003", "hw_lzdl" -> "100000004", "hw_csgjhwbfoyo" -> "100000005",
    "hw_lzdlhwb" -> "100000006", "hw_tw_lzdl" -> "100000007", "hw_tw_lzdl3" -> "100000008",
    "hw_tw_lzdl4" -> "100000009", "hw_jt_wyfx" -> "100000010", "hw_web_pay" -> "100000011",
    "hw_tw_lzdl5" -> "100000012", "hw_tw_android_yjqy" -> "100000013", "hw_tw_ios_yjqy" -> "100000014",
    "hw_sgqj" -> "100000015", "hw_tw_ios_sgqj" -> "100000016", "hw_wzsy" -> "100000017",
    "hw_hw_android_HWwzsy" -> "100000018", "hw_hw_tw_android_HW_jyqy" -> "100000019", "hw_csssj" -> "100000020",
    "hw_XJZZ" -> "100000021", "hw_xjzz_standalone" -> "100000022", "hw_hw_tw_google_xjzz" -> "100000023",
    "hw_hw_tw_standalone_xjzz" -> "100000024", "hw_hw_tw_standalone_sgqj_test" -> "100000025",
    "hw_hw_tw_google" -> "100000026", "hw_hw_tw_google_sgqj" -> "100000027", "hw_hw_tw_standalone_sgqj" -> "100000028",
    "hw_hw_tw_ios_xjzz" -> "100000029")

  /**
    * 关联上独代的数据channel_id ,game_channel_id,game_id一共两个接口，其中一个belong的是IOS海外一些没有走独代的数据，另外一个接口是走独代的数据关联接口
    */
  def transform(rdd: RDD[(Document, Boolean)]
                , bGameChannel: Broadcast[Map[String, (String, String, String)]]
                , bRmb: Broadcast[Map[String, Double]]
                , bUsd: Broadcast[Map[String, Double]]
                , bGcmMap: Broadcast[Map[String, String]]
                , service: String
                , bIOS128: Broadcast[Set[String]]): RDD[(Document, Boolean)] = {

    service match {
      case "haiwai" =>
        rdd.map { case (doc, flag) =>
          if (flag) {
            val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
            val agent = JunhaiLog.getDocument(doc, JunhaiLog.agent)
            //开始关联
            var columnValue = JunhaiLog.getString(game, JunhaiLog.game_id) //默认的独代的channel_id字段
            columnValue = HWGameMap.getOrElse(columnValue, columnValue)
            val values = bGameChannel.value.getOrElse(columnValue, (error, error, error))
            val gameChannelId = values._1
            val channelId = values._2
            val gameId = values._3

            game.put("app_id", gameId) //这条语句必须要在上面
            //            game.put(JunhaiLog.game_id,gameId)
            game.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId, error))

            agent.put(JunhaiLog.channel_id, channelId)
            agent.put(JunhaiLog.game_channel_id, gameChannelId)

            val event = JunhaiLog.getString(doc, JunhaiLog.event) //添加汇率转换
            if (event == JunhaiLog.order) {
              val order = JunhaiLog.getDocument(doc, JunhaiLog.order)
              val currencyAmount = JunhaiLog.getDouble(order, JunhaiLog.currency_amount)
              val currencyType = JunhaiLog.getString(order, JunhaiLog.currency_type)
              val rmb = bRmb.value.getOrElse(currencyType, 1.0) * currencyAmount
              val usd = bUsd.value.getOrElse(currencyType, 1.0) * currencyAmount
              order.append(JunhaiLog.cny_amount, rmb)
              order.append(JunhaiLog.usd_amount, usd)
            }

            doc.put(JunhaiLog.agent, agent)
            (doc, flag)
          } else {
            (doc, flag)
          }
        }
      case _ =>
        println("dalan")
        rdd.map { case (doc, flag) =>
          if (flag) {
            val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
            val agent = JunhaiLog.getDocument(doc, JunhaiLog.agent)
            val columnValue = JunhaiLog.getString(game, JunhaiLog.game_id) //默认的独代的channel_id字段
            val values = bGameChannel.value.getOrElse(columnValue, (error, error, error))
            val gameChannelId = values._1
            val channelId = values._2
            val gameId = values._3

            game.put("app_id", gameId)
            //            game.replace(JunhaiLog.game_id,gameId)
            //            newGame.put(JunhaiLog.game_name,JunhaiLog.getString(game,JunhaiLog.game_name))
            //            newGame.put(JunhaiLog.game_ver,JunhaiLog.getString(game,JunhaiLog.game_ver))
            //            game.put(JunhaiLog.game_id,gameId)
            //            println("columnValue"+columnValue)
            game.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId, error))
            agent.put(JunhaiLog.channel_id, channelId)
            agent.put(JunhaiLog.game_channel_id, gameChannelId)
            doc.put(JunhaiLog.agent, agent)

            if (bIOS128.value.contains(gameChannelId)&&doc.getString(JunhaiLog.event).equals("login")) { //过滤出小于128的ios独代
              (doc, flag)
            } else {
              (doc, false)
            }
          } else {
            (doc, flag)
          }
        }
    }
  }
}
