package com.ijunhai.batch

import java.util.Date

import com.ijunhai.storage.redis.RedisSink
import com.ijunhai.common.logsystem.JunhaiLog
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by Admin on 2017-11-01.
  */
object NewUserIMEILoad {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    val sourceFilePath=args(0)
    val destGameChannelId=args(1)
    val destFilePath=args(2)
    val destRegDate= args(3).split(',').toSet

    val sparkConf=new SparkConf()
      //      .setMaster("local")
      .setAppName("OldLoginLogBatch")
    val sc=new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val rdd=sc.textFile(sourceFilePath)
    val document = rdd.map(string => {
      try {
        val doc = Document.parse(string)
        (doc, true)
      } catch {
        case e: Exception =>
          System.err.println(new Date()+" Error: json parse error!")
          (new Document("log", string), false)
      }
    })
    val redisSinkCluster = sc.broadcast(RedisSink.apply())

    val data=document.filter(_._2).map(line=>{
      val doc=line._1
      val serverDateDay=JunhaiLog.getString(doc,JunhaiLog.server_date+"_day")
      val regDate=JunhaiLog.getString(doc,JunhaiLog.reg_date)
      val gameChannelId=doc.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.game_channel_id)
      (doc.get(JunhaiLog.device).asInstanceOf[Document].getString(JunhaiLog.android_imei),regDate==serverDateDay,gameChannelId==destGameChannelId,destRegDate.contains(regDate),regDate)
    }).filter(line=> line._2 && line._3 && line._4).cache()
    for (date <- destRegDate){
      data.filter(_._5==date).map(_._1).repartition(1).saveAsTextFile(destFilePath+"/"+date)
    }
    data.unpersist()
  }
}
