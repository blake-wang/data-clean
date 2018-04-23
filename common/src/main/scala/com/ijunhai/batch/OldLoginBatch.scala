package com.ijunhai.batch

import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import com.ijunhai.process.agent.{KafkaLogProcess, AgentProcess}
import com.ijunhai.common.ip.IPAddress
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.ip.IP
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

import scala.collection.JavaConversions._

/**需要剔除自有渠道的channel_id
  * Created by Admin on 2017-09-28.
  */
object OldLoginBatch {

  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    val game_id="game_id"
    val game_channel_id="game_channel_id"
    val channel_id="channel_id"
    val sub_pf="sub_pf"
    val column="channel_id"
    val error="error"
    val game_ ="game"
    val agent="agent"

    val sparkConf=new SparkConf()
//      .setMaster("local")
      .setAppName("OldLoginBatch")
    val sc=new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val hdfsCachePath=args(1)
    val ipDatabasePath=args(2)
    val agcTuple=AgentProcess.getAgentGameChannel(hdfsCachePath)
    val acMap=AgentProcess.getAgentChannel(hdfsCachePath)
    val scMap=AgentProcess.getSubChannel(hdfsCachePath)
    val bscMap=AgentProcess.getAllBySubChannel(hdfsCachePath)
    val ggMap=agcTuple._1
    val gcMap=agcTuple._2

    val bAcMap=sc.broadcast(acMap)
    val bGgMap=sc.broadcast(ggMap)
    val bGcMap=sc.broadcast(gcMap)
    val bScMap=sc.broadcast(scMap)
    val bBscMap=sc.broadcast(bscMap)
    val fileDate=args(3)
    val rdd1=sc.textFile(args(0))
    val bBytes=sc.broadcast(IPAddress.init(ipDatabasePath))
    val acc_game_channel_id=sc.longAccumulator
    val document = rdd1.map(string => {
      try {
        val doc = Document.parse(string)
//        acc_right.add(1)
        (doc, true)
      } catch {
        case e: Exception =>
//          acc_error.add(1)
          println(string)
          (new Document("log", string), false)
      }
    })
//    println(fileDate+" json parse error count:"+document.filter(!_._2).count())
//    println(fileDate+" json parse right count:"+document.filter(_._2).count())
//    document.unpersist()


//    println(fileDate+" filter date:"+document.count())
    val filterDocuments=document.filter(line=>{
      val document=line._1
      val game=document.get(game_).asInstanceOf[Document]
      val gameId=game.getString(game_id)
      val agent_ =document.get(agent).asInstanceOf[Document]
      val channelId=agent_.getString(column)
      val server_ts=JunhaiLog.getTimestamp(document,"server_ts")

      line._2 && gameId!=null && channelId!=null &&  server_ts>0
    })
//    val filterDate=no_filterDocuments.filter(line=>{
//      val document=line._1
//      val server_ts=JunhaiLog.getTimestamp(document,"server_ts")
//      val serverDate = TimeUtil.time2DateString(Save2Redis.FORMAT_STR, server_ts, TimeUtil.SECOND)
//      serverDate=="2017-09-25"
//    })
//    println("filterDate:"+filterDate.count())
//    println(fileDate+" filter right:"+filterDocuments.count())
val redisSinkCluster = sc.broadcast(RedisSink.apply())

    val resultDocuments=filterDocuments.mapPartitions(p=> {
      val ipClass = new IP
      ipClass.load(bBytes.value)
      val temp = p.map { case (document, result) => {
          try {
            val geo = IPAddress.getIPAddress(document.getString(JunhaiLog.client_ip), ipClass)
            for (key <- geo.keySet()) {
              document.put(key, geo.get(key))
            }
          }catch {
            case e:Exception=>
              document.put("province", "")
              document.put("country","")
              document.put("city","")
          }
        (document, result)
      }}
      temp
    }).map(order=>{
//      acc_filter.add(1)
      //        if(bCopyMap.value.nonEmpty){
      //          order._1.append(create_ip,bCopyMap.value.get(orderSn,"0.0.0.0"))
      //        }
      //开始关联
      val game=order._1.get(game_).asInstanceOf[Document]
      val gameIdStr=game.getString(game_id)

      val agent_ =order._1.get(agent).asInstanceOf[Document]
      val device=order._1.get("device").asInstanceOf[Document]
      val gameChannelId=agent_.getString(game_channel_id)
      if (gameChannelId==""){
        val columnValue=agent_.getString(column) //默认的独代的channel_id字段
        if(JunhaiLog.isNumber(gameIdStr)){
          val gameId=gameIdStr.toInt
          if (gameId>=63){//此时order表中的channel_id存放的是game_channel_id的值 需要得到channel_id
            val value=bGgMap.value.getOrElse(gameId+columnValue,error)
            val subPf=bAcMap.value.getOrElse(value,error)
            agent_.put(channel_id,value)
            agent_.put(game_channel_id,columnValue)//将原来的字段名字改回来
            device.put(sub_pf,subPf)
            val osType= subPf match {
              case "0" =>
                "android"
              case "1" =>
                "ios"
              case "2" =>
                "ios"
              case "3" =>
                "ios"
              case "4" =>
                "android"
              case "5" =>
                "ios"
              case "6" =>
                "ios"
              case _ =>
                subPf
            }
            device.put(JunhaiLog.os_type,osType)
          } else {
            if (!bScMap.value.contains(columnValue)) {
              //此时order表中的channel_id就是channel_id
              val value = bGcMap.value.getOrElse(gameId + columnValue,error) //拿到game_channel_id
              val subPf = bAcMap.value.getOrElse(columnValue,error)
              agent_.put(game_channel_id, value)
              device.put(sub_pf,subPf)
              val osType= subPf match {
                case "0" =>
                  "android"
                case "1" =>
                  "ios"
                case "2" =>
                  "ios"
                case "3" =>
                  "ios"
                case "4" =>
                  "android"
                case "5" =>
                  "ios"
                case "6" =>
                  "ios"
                case _ =>
                  subPf
              }
              device.put(JunhaiLog.os_type,osType)
            } else {
              val value=bBscMap.value.getOrElse(columnValue,error)//根据sub_channel拿game_channel_id
              agent_.put(game_channel_id, value)
              val channelId=bScMap.value.getOrElse(columnValue,error)
              agent_.put(channel_id,channelId)
              val subPf = bAcMap.value.getOrElse(channelId,error)
              device.put(sub_pf,subPf)
              val osType= subPf match {
                case "0" =>
                  "android"
                case "1" =>
                  "ios"
                case "2" =>
                  "ios"
                case "3" =>
                  "ios"
                case "4" =>
                  "android"
                case "5" =>
                  "ios"
                case "6" =>
                  "ios"
                case _ =>
                  subPf
              }
              device.put(JunhaiLog.os_type,osType)
            }
          }
        }else{
          val value=bGgMap.value.getOrElse(gameIdStr+columnValue,error)
          val subPf=bAcMap.value.getOrElse(value,error)
          agent_.put(channel_id,value)
          agent_.put(game_channel_id,columnValue)//将原来的字段名字改回来
          device.put(sub_pf,subPf)
          val osType= subPf match {
            case "0" =>
              "android"
            case "1" =>
              "ios"
            case "2" =>
              "ios"
            case "3" =>
              "ios"
            case "4" =>
              "android"
            case "5" =>
              "ios"
            case "6" =>
              "ios"
            case _ =>
              subPf
          }
          device.put(JunhaiLog.os_type,osType)
        }
      }else{
        acc_game_channel_id.add(1)
      }
      order._1
    })

    val agentWithoutDalan=resultDocuments.filter{case bson=>{//剔除大蓝渠道的数据
      !JunhaiLog.junhaiChannelsId.contains(bson.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id))
    }}
//    println("agentWithoutDalan:"+agentWithoutDalan.count())
//    println(resultDocuments.count())
//    resultDocuments.take(10).foreach(println)

    val withNewUserFlag=Save2Redis.saveDStream(agentWithoutDalan,"agent",redisSinkCluster)
//    println(fileDate+" write count:"+agentWithoutDalan.map(line=>DataProcess.documentFlat("",line)).filter(line=>{
//      val serverTs=JunhaiLog.getInt(line,"server_ts")
//      val serverDate = TimeUtil.time2DateString(Save2Redis.FORMAT_STR, serverTs.toInt, TimeUtil.SECOND)
//        JunhaiLog.getString(line,"agent-channel_id")=="10192" && JunhaiLog.getString(line,"event")=="login" && JunhaiLog.getString(line,"game-game_id")=="121" && JunhaiLog.getString(line,"is_test")=="regular" && serverDate=="2017-09-25"
//    }).map(line=>JunhaiLog.getString(line,"user-user_id")).distinct().count())

//    println("ac_error:"+acc_error)
//    println("ac_right:"+acc_right)
//    println("acc_filter:"+acc_filter)
//    println("acc_game_channel_id:"+acc_game_channel_id)
    withNewUserFlag.map(line=>{
      val result=if(line.getString(JunhaiLog.event)==JunhaiLog.eventActive){
        KafkaLogProcess.documentFlat("",line).append("user-user_id","")//kylin中需要这个字段来对其数据，实际上激活是没有用户ID这个字段的
      }else{
        KafkaLogProcess.documentFlat("",line)
      }
      result.toJson
    }).saveAsTextFile("/data/agent/login/"+fileDate)
//    Save2Kafka.saveDStreamAsync(withNewUserFlag.map(line=>DataProcess.documentFlat("",line).toJson()),topic)
//      Save2Kafka.saveDStreamTransaction(withNewUserFlag.map(_.toJson()),topic,topics,offset,nameServer,group)
//      withNewUserFlag.foreach(println)

  }
}
