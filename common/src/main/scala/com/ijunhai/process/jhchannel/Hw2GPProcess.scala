package com.ijunhai.process.jhchannel

import java.util

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem._
import com.ijunhai.process.agent.{AgentProcess, KafkaLogProcess}
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.kafka.KafkaSink
import com.ijunhai.storage.redis.RedisSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

/**
  * Created by Admin on 2017-11-09.
  */
object Hw2GPProcess {
  val HAIWAI = "haiwai"
  val HAIWAI_DATABASE = "youyun_hw"
  val HAIWAI_TABLE_LOGIN_NAME = "hw_login_log"
  val HAIWAI_TABLE_ACTIVE_NAME = "hw_active_log"
  val HAIWAI_TABLE_AF_NAME ="af_ad_active"
  val HAIWAI_TABLE_VOIDED_ORDER_NAME="voided_order"

  def process(rDD: RDD[String], kafkaSink: Broadcast[KafkaSink]
              , bGreenPlumSink: Broadcast[GreenPlumSink]
              , topic: String
              , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    val game_pack=AgentProcess.getPackName()
    val pack_name=rDD.sparkContext.broadcast(game_pack)
    val allLogs = rDD.map(log => {
      val document = try {
        JunhaiLog.log2bson(log)
      } catch {
        case e: Exception =>
          (new Document(KafkaLogProcess.LOG, log), false)
      }
      val event = if (document._2) {
        JunhaiLog.getString(document._1, JunhaiLog.event)
      } else {
        ""
      }
      val event_type=if(document._2){
        JunhaiLog.getString(document._1,JunhaiLog.event_type)
      }
      else{
        ""
      }
      val event_order=if(document._2){
        JunhaiLog.getString(document._1,JunhaiLog.event_order)
      }else{
        ""
      }
      val void_order=if (document._2){
        JunhaiLog.getString(document._1,HwVoidOrder.google_lf)
      }
      else{
        ""
      }
      (document._1,event,event_type,event_order,void_order,document._2)

    })
      val rightAllLogs = allLogs.filter(_._6)
      val errorAllLogs = allLogs.filter(!_._6)
    //    Save2Kafka.saveMetrics(kafkaSink,JunhaiLog.MONITOR_TOPIC,new Date(),Log2GPProcess.getClass.getName,JunhaiLog.INFO,"Log2GP all error log","Num",errorAllLogs.count()+"")
    //    Save2Kafka.saveMetrics(kafkaSink,JunhaiLog.MONITOR_TOPIC,new Date(),Log2GPProcess.getClass.getName,JunhaiLog.INFO,"Log2GP login error log","Num"
    //      ,errorAllLogs.filter(_._2==JunhaiLog.eventLogin).count()+"")
    //    Save2Kafka.saveDStreamAsync(errorAllLogs.map(_._1.toJson()),kafkaSink,topic+"Login"+"Error")
      val loginLogs = rightAllLogs.filter(_._2 == JunhaiLog.eventLogin).map(_._1)
    //val activeLogs = rightAllLogs.filter(_._2 == JunhaiLog.eventActive).map(_._1)

      val  afLogs=rightAllLogs.filter(_._3==JunhaiLog.eventAf).map(_._1).map(document=>{
      val bundle_id=JunhaiLog.getString(document,HaiwaiAF.bundle_id)
      val game_id=pack_name.value.getOrElse(bundle_id,null)
      document.append(HaiwaiAF.game_id,game_id)
      document
    })
      val voidedOrder=rightAllLogs.filter(_._4==JunhaiLog.event_order).map(_._1)

      val voided_google=rightAllLogs.filter(_._5==HwVoidOrder.google_lf).map(_._1).map(documet=>{
          val pack_name=JunhaiLog.getString(documet,HaiwaiGoogle.package_name)
          val google_data=documet.get("google_data").asInstanceOf[Document].get("voidedPurchases").asInstanceOf[util.ArrayList[Document]]
          for(i<- 0 until google_data.size()){
              val purchaseToken=JunhaiLog.getString(google_data.get(i),HaiwaiGoogle.purchaseToken)
              val  voidedTimeMillis=JunhaiLog.getLong(google_data.get(i),HaiwaiGoogle.voidedTimeMillis)
              val purchaseTimeMillis= JunhaiLog.getLong(google_data.get(i),HaiwaiGoogle.purchaseTimeMillis)
              val hour=(voidedTimeMillis-purchaseTimeMillis)/3600/1000
              val voided_time=TimeUtil.time2SqlDate(JunhaiLog.getLong(google_data.get(i),HaiwaiGoogle.voidedTimeMillis),TimeUtil.MILLISECOND)
              val voided_date=TimeUtil.time2DateString("yyyyMMdd",JunhaiLog.getLong(google_data.get(i),HaiwaiGoogle.voidedTimeMillis),TimeUtil.MILLISECOND).toInt
              val d=new Document()
              d.put(HaiwaiGoogle.package_name,pack_name)
              d.put(HaiwaiGoogle.purchaseToken,purchaseToken)
              if(Save2GP.SelectDStream(d,bGreenPlumSink,HAIWAI_TABLE_VOIDED_ORDER_NAME)>0){

                 d.put(HaiwaiGoogle.voidedTimeMillis,voided_time)
                 d.put(HwVoidOrder.voided_time,voided_date)
                 d.put("interval",hour)
                //d.put(HwVoidOrder.voided_time,voided_date)
                 Save2GP.UpdateDStream(d,bGreenPlumSink,HAIWAI_TABLE_VOIDED_ORDER_NAME)
         }
       }
    }).count()
    //GP
    val loginDocument = loginTransfor(loginLogs, bGreenPlumSink, Hw2GPProcess.HAIWAI_TABLE_LOGIN_NAME).count()
   // val activeDocument = activeTransfor(activeLogs, bGreenPlumSink, topic, redisSinkCluster, kafkaSink)
    val afDocument=afTransfor(afLogs,bGreenPlumSink,Hw2GPProcess.HAIWAI_TABLE_AF_NAME).count()

    val gooleOrder=voidedTransfor(voidedOrder,bGreenPlumSink,Hw2GPProcess.HAIWAI_TABLE_VOIDED_ORDER_NAME).count()
    //kafka
    //Save2Kafka.saveDStreamAsync(afDocument.map(_.toJson()), kafkaSink, topic + "AF", "com.ijunhai.process.jhchannel.Hw2GPProcess")
   // Save2Kafka.saveDStreamAsync(afDocument.map(_.toJson()), kafkaSink, topic + "Active", "com.ijunhai.process.jhchannel.Hw2GPProcess")
    //Save2Kafka.saveDStreamAsync(loginDocument.map(_.toJson()), kafkaSink, topic + "Login", "com.ijunhai.process.jhchannel.Hw2GPProcess")


  }

  def loginTransfor(rDD: RDD[Document], bGreenPlumSink: Broadcast[GreenPlumSink], table: String): RDD[Document] = {
      val documents: RDD[Document] = rDD.map(doc => {
      val haiwaiLogin = HaiwaiLogin.load(doc)
      HaiwaiLogin.unload(haiwaiLogin)

    })
//    documents.foreach(println(_))//-------------------
     Save2GP.saveDStream(documents, bGreenPlumSink, table)
  }
  def afTransfor(rDD: RDD[Document],bGreenPlumSink:Broadcast[GreenPlumSink],table:String):RDD[Document]={
    val documents = rDD.map(document => {
      HaiwaiAF.unload(HaiwaiAF.load(document))
    })
   // documents.foreach(println(_)) //-------------------------
    Save2GP.saveDStream(documents, bGreenPlumSink, table)
  }

  def voidedTransfor(rDD: RDD[Document],bGreenPlumSink:Broadcast[GreenPlumSink],table:String):RDD[Document]={
    val documents = rDD.map(document => {
      HwVoidOrder.unload(HwVoidOrder.load(document))
    })
    // documents.foreach(println(_)) //-------------------------
    Save2GP.saveDStream(documents, bGreenPlumSink, table)
  }
}
