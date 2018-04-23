package com.ijunhai.process.jhchannel

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.redis.{RedisSinkAlone, RedisSink, Save2Redis}
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.common.offset.redisRocketMQOffset
import com.ijunhai.process.agent.AgentProcess
import com.ijunhai.common.serde.MessageSerde
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.main.jhchannel.AdvClick
import com.ijunhai.common.redis.RedisClientAlone
import com.ijunhai.common.{MD5Util, TimeUtil}
import com.ijunhai.process.CommonProcess
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{OffsetRange, TopicQueueId}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConversions._

/**
  * Created by Admin on 2017-11-08.
  */
object ClickProcess {
  val advClick = "advClick"
  val junhai_adid = "junhai_adid"
  val device_id = "device_id"
  val game_ = "game"
  val client_ip = "client_ip"
  val click_time = "click_time"
  val DL_DATABASE = "youyun_ad"
  val CM_DATABASE = "chumeng_ad"
  val DETIAL_DATABASE = "click_detail"
  val TABLE_NAME = "ad_click"
  val PAGE_CLICK_DOWN_TABLE = "page_click_down"
  val company_id = "company_id"
  val event_tag = "event_tag"
  val apk_name = "apk_name"

  def pageProcess(rdd: RDD[MessageExt]
                  , kafkaSink: Broadcast[KafkaSink]
                  , redisSink: Broadcast[RedisSinkAlone]
                  , CmGreenPlumSink: Broadcast[GreenPlumSink]
                  , topic: String
                  , offset: java.util.Map[TopicQueueId, Array[OffsetRange]]
                  , nameServer: String
                  , topics: java.util.List[String]
                  , group: String
                  , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    if (!rdd.isEmpty()) {
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      //      Save2Kafka.saveMetrics(kafkaSink,JunhaiLog.MONITOR_TOPIC,new Date(),"com.ijunhai.kafka.mq2kafka.AdvClickProcess",JunhaiLog.INFO,s"Total get from rocketMQ $nameServer and topic(s) $topicsStr","Num",rdd.count()+"")

      val temp = CommonProcess.extraDataWithCollectTime(rdd, kafkaSink, nameServer, topicsStr)
      val data = temp._1
      val msgs = data.map(_._1)
      Save2Kafka.saveDStreamAsync(msgs, kafkaSink, topic, "com.ijunhai.mq2kafka.PageClickProcess")

      val document = pageAdvLogAnalysis(msgs)
      Save2Kafka.saveDStreamAsync(document.filter(!_._2).map(_._1.toJson()), kafkaSink, topic + AgentProcess.ERROR_FLAG, nameServer + ":com.ijunhai.mq2kafka.PageClickProcess")
      //GP
      val cmClickCount = pageGetCount(document.filter(_._2).map(_._1))
      val clickCountResult = Save2GP.saveDStream(cmClickCount, CmGreenPlumSink, PAGE_CLICK_DOWN_TABLE)

      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), "com.ijunhai.mq2kafka.PageClickProcess", JunhaiLog.INFO, s"$topicsStr after GP ", clickCountResult.count() + "")

      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }

  def pageGetCount(rDD: RDD[Document]): RDD[Document] = {
    val connectSignal = '`'
    rDD.map(document => {
      val clickTime = JunhaiLog.getString(document, click_time)
      (clickTime.substring(0, clickTime.length - 3)
        + connectSignal + JunhaiLog.getString(document, event_tag)
        + connectSignal + JunhaiLog.getString(document, apk_name), 1L)
    }).reduceByKey(_ + _).map(line => {
      val keys = line._1.split(connectSignal)
      val time = TimeUtil.dateString2Time("yyyy-MM-dd HH:mm", keys(0), TimeUtil.SECOND)
      val date = TimeUtil.time2DateString("yyyyMMdd", time, TimeUtil.SECOND).toInt
      val hour = TimeUtil.time2DateString("HH", time, TimeUtil.SECOND).toInt
      val minute = TimeUtil.time2DateString("mm", time, TimeUtil.SECOND).toInt
      val eventTag = keys(1)
      val apkName = keys(2)
      val click = line._2.toInt
      val document = new Document().append(event_tag, eventTag).append("click_date", date)
        .append("click_hour", hour).append("click_minute", minute).append("click_cnt", click).append(apk_name, apkName)
      val rowData = line._1 + line._2
      document.put("id", MD5Util.getMD5(rowData))
      document
    })
  }


  def process(rdd: RDD[MessageExt]
              , kafkaSink: Broadcast[KafkaSink]
              , redisSink: Broadcast[RedisSinkAlone]
              , gpSinkList: java.util.List[Broadcast[GreenPlumSink]]
              , topic: String
              , offset: java.util.Map[TopicQueueId, Array[OffsetRange]]
              , nameServer: String
              , topics: java.util.List[String]
              , group: String
              , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    if (!rdd.isEmpty()) {
      val miniteTime = TimeUtil.time2DateString("yyyy-MM-dd-HH:mm", new Date().getTime, TimeUtil.MILLISECOND)
      val currentTime = new Date().getTime / 1000
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), "com.ijunhai.kafka.mq2kafka.AdvClickProcess", JunhaiLog.INFO, s"Total get from rocketMQ $nameServer and topic(s) $topicsStr", "Num", rdd.count() + "")

      val temp = CommonProcess.extraDataWithCollectTime(rdd, kafkaSink, nameServer, topicsStr)
      val data = temp._1
      val ips = temp._2
      val msgs = data.map(_._1)

      val document: RDD[(Document, Boolean)] = advLogAnalysis(msgs).map(line => {
        (line._1, JunhaiLog.isNumber(JunhaiLog.getString(line._1, junhai_adid)) && line._2)
      })
      //明细到Kafka
      val kafkaDetail: RDD[String] = getDetail2Kafka(document.filter(_._2).map(_._1))
      Save2Kafka.saveDStreamAsync(kafkaDetail, kafkaSink, topic, "com.ijunhai.kafka.mq2kafka.AdvClickProcess")
      Save2Kafka.saveDStreamAsync(document.filter(!_._2).map(_._1.toJson()), kafkaSink, topic + AgentProcess.ERROR_FLAG, nameServer + ":com.ijunhai.kafka.mq2kafka.AdvClickProcess")
      //GP和redis
      serviceProcess(document.filter(_._2).map(_._1), kafkaSink, redisSink, gpSinkList, topicsStr, miniteTime)

      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
        val jedis = RedisClientAlone.getInstatnce.getJedis
        ips.foreach(ip => {
          jedis.set(AdvClick.fileCollectLastTime + ip, currentTime + "") //更新每个ip的时间
        })
        RedisClientAlone.returnResource(jedis)
      }
    }
  }

  def processTest(rdd1: RDD[MessageExt]
                  , kafkaSink: Broadcast[KafkaSink]
                  , gpSinkList: java.util.List[Broadcast[GreenPlumSink]]
                  , topic: String
                  , offset: java.util.Map[TopicQueueId, Array[OffsetRange]]
                  , nameServer: String
                  , topics: java.util.List[String]
                  , group: String): Unit = {
    if (!rdd1.isEmpty()) {
      val temp = CommonProcess.extraDataWithCollectTimeTest(rdd1, kafkaSink, nameServer, topic)
      val document: RDD[(Document, Boolean)] = advLogAnalysis(temp.map(_._1)).map(line => {
        (line._1, JunhaiLog.isNumber(JunhaiLog.getString(line._1, junhai_adid)) && line._2)
      })
      val add: RDD[String] = getDetail2Kafka(document.filter(_._2).map(_._1))
      Save2Kafka.saveDStreamAsync(add, kafkaSink, "advTest", "com.ijunhai.kafka.mq2kafka.AdvClickProcess")

    }
  }

  def monitorTest(rdd1: RDD[MessageExt]
                  , kafkaSink: Broadcast[KafkaSink]
                  , topic: String
                  , nameServer: String
                  , topics: java.util.List[String]
                  , group: String): Unit = {
    if (!rdd1.isEmpty()) {
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      val msgs: RDD[(String, String)] = CommonProcess.extraData(rdd1, kafkaSink, nameServer, topicsStr)
      msgs.mapPartitions(a=>a.map(b=>
        kafkaSink.value.syncSend("log", b.toString())
      )).count()


    }
  }




  def getDetail2Kafka(rDD: RDD[Document]): RDD[String] = {
    //id,game ,channel ,junhai_adid ,client_ip ,company_id ,click_time ,click_source
    rDD.map(document => {
      val time = TimeUtil.dateString2Time("yyyy-MM-dd HH:mm", JunhaiLog.getString(document, click_time), TimeUtil.SECOND)
      val hour = TimeUtil.time2DateString("HH", time, TimeUtil.SECOND)
      new Document().append("id", MD5Util.getMD5(document.toJson())).append(game_, JunhaiLog.getString(document, game_))
        .append("channel", JunhaiLog.getString(document, "channel"))
        .append(junhai_adid, JunhaiLog.getString(document, junhai_adid))
        .append(client_ip, JunhaiLog.getString(document, client_ip))
        .append(company_id, JunhaiLog.getString(document, company_id))
        .append(click_time, JunhaiLog.getString(document, click_time))
        .append("click_source", document.toJson)
        .append("hour", hour).toJson()
    })
  }


  def serviceProcess(rDD: RDD[Document], kafkaSink: Broadcast[KafkaSink]
                     , redisSink: Broadcast[RedisSinkAlone], gpSinkList: java.util.List[Broadcast[GreenPlumSink]]
                     , topicsStr: String, miniteTime: String): Unit = {

    val dlGPSink = gpSinkList.get(0)
    val cmGPSink = gpSinkList.get(1)
    val detailGPSink = gpSinkList.get(2)

    val rdd = rDD.filter(log => log.get("game").toString.startsWith("U"))

    val gpDetail = getDetail2GP(rdd).filter(_._2).map(_._1)
    val afterSave2GP = Save2GP.saveDStream(gpDetail, detailGPSink, "detail")

    val dlClickCount = getCount(rdd.filter(_.get("company_id").toString.equals("1")), miniteTime)
    val cmClickCount = getCount(rdd.filter(_.get("company_id").toString.equals("5")), miniteTime)
    val dlClickCountResult = Save2GP.saveDStream(dlClickCount, dlGPSink, TABLE_NAME)
    val cmClickCountResult = Save2GP.saveDStream(cmClickCount, cmGPSink, TABLE_NAME)

    val clickDetail = getDetail(rDD)
    val clickDetailResult = Save2Redis.saveKeyValueAlone(clickDetail, redisSink, 3 * 24 * 60 * 60) //详细数据放redis去重
    Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), "com.ijunhai.kafka.mq2kafka.AdvClickProcess", JunhaiLog.INFO, s"$topicsStr Total send to redis clickDetailResult Num is", afterSave2GP.count() + "")
    Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), "com.ijunhai.kafka.mq2kafka.AdvClickProcess", JunhaiLog.INFO, s"$topicsStr Total send to redis clickCountResult Num is", "dl:" + dlClickCountResult.count() + " cm:" + cmClickCountResult.count() + "")
    Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), "com.ijunhai.kafka.mq2kafka.AdvClickProcess", JunhaiLog.INFO, s"$topicsStr Total send to redis clickDetailResult Num is", clickDetailResult.count() + "")
  }

  def getDetail2GP(rDD: RDD[Document]): RDD[(Document, Boolean)] = {
    //id,game ,channel ,junhai_adid ,client_ip ,company_id ,click_time ,click_source
    rDD.map(document => {
      val doc = new Document().append("id", MD5Util.getMD5(document.toJson())).append(game_, JunhaiLog.getString(document, game_))
        .append("channel", JunhaiLog.getString(document, "channel"))
        .append(junhai_adid, JunhaiLog.getString(document, junhai_adid))
        .append(client_ip, JunhaiLog.getString(document, client_ip))
        .append(company_id, JunhaiLog.getString(document, company_id))
        .append(click_time, TimeUtil.dataString2SqlDate("yyyy-MM-dd HH:mm:ss", JunhaiLog.getString(document, click_time)))
        .append("click_source", document.toJson)
      var flag = true
      if (JunhaiLog.getString(document, "channel").equals("")) {
        flag = false
      }
      (doc, flag)
    })
  }


  def getDetail(rDD: RDD[Document]): RDD[(String, String)] = {
    rDD.map(document => {
      val deviceId = JunhaiLog.getString(document, device_id)
      val game = JunhaiLog.getString(document, game_)
      val clientIp = JunhaiLog.getString(document, client_ip)
      val companyId = JunhaiLog.getString(document, company_id)

      val head = company_id + "_" + companyId + "_api_click_detail_" + game + "_"
      if (deviceId == "") {
        Array((head + clientIp, document.toJson()))
      } else {
        Array(
          (head + clientIp, document.toJson()),
          (head + deviceId, document.toJson())
          //(head + MD5Util.getMD5(deviceId), document.toJson()),
          //(head + MD5Util.getLowCaseMD5(deviceId), document.toJson())
        )
      }
    }).flatMap(line => line)
  }

  /**
    * 获得广告位的点击数据
    *
    * @param rDD
    * @param miniteTime
    * @return
    */
  def getCount(rDD: RDD[Document], miniteTime: String): RDD[Document] = {
    val connectSignal = '|'
    rDD.map(document => {
      val clickTime = JunhaiLog.getString(document, click_time)
      (JunhaiLog.getString(document, junhai_adid)
        + connectSignal + clickTime.substring(0, clickTime.length - 3)
        + connectSignal + JunhaiLog.getString(document, "game")
        + connectSignal + JunhaiLog.getString(document, "pf")
        + connectSignal + JunhaiLog.getString(document, "channel"), 1L)
    }).reduceByKey(_ + _)
      //      .map(line=>(line._1,line._2+""))
      .map(line => {
      val keys = line._1.split(connectSignal)
      val time = TimeUtil.dateString2Time("yyyy-MM-dd HH:mm", keys(1), TimeUtil.SECOND)
      val date = TimeUtil.time2DateString("yyyyMMdd", time, TimeUtil.SECOND).toInt
      val hour = TimeUtil.time2DateString("HH", time, TimeUtil.SECOND).toInt
      val minute = TimeUtil.time2DateString("mm", time, TimeUtil.SECOND).toInt
      var game = ""
      var channel = ""
      var rawPf = ""
      if (keys.length == 3) {
        game = keys(2)
      } else if (keys.length == 4) {
        game = keys(2)
        rawPf = keys(3)
      } else if (keys.length == 5) {
        game = keys(2)
        rawPf = keys(3)
        channel = keys(4)
      } else if (keys.length > 5) {
        game = keys(2)
        rawPf = keys(3)
        for (i <- 4 until keys.length) {
          channel += keys(i)
        }
      }
      val pf = if (rawPf == null || rawPf == "") 2 else rawPf.toInt
      val platform = 4
      val click = line._2.toInt
      val junhaiAdId = if (keys(0) == null || keys(0) == "") {
        0
      } else {
        keys(0).toInt
      }

      val document = new Document().append(junhai_adid, junhaiAdId).append("click_date", date)
        .append("click_hour", hour).append("click_minute", minute).append("click_cnt", click).append("game", game).append("channel", channel).append("pf", pf)
        .append("platform", platform)
      val rowData = line._1 + line._2
      document.put("id", MD5Util.getMD5(rowData))
      document
    })
  }

  def advLogAnalysis(rDD: RDD[String]): RDD[(Document, Boolean)] = {
    rDD.map(log => {
      val (time, ip, json) = getAdvContent(log)
      try {
        val doc = Document.parse(json)
        doc.put(client_ip, ip)
        if (!doc.containsKey(click_time)) {
          doc.put(click_time, time)
        }
        if (json.contains(company_id) && json.contains(junhai_adid)) {
          (doc, true)
        } else {
          (doc, false)
        }
      } catch {
        case e: Exception =>
          System.err.println(new Date() + " Error: json parse error!")
          (new Document(AgentProcess.LOG, json), false)
      }
    })
  }

  def pageAdvLogAnalysis(rDD: RDD[String]): RDD[(Document, Boolean)] = {
    rDD.map(str => {
      try {
        val nginx_time = str.substring(str.indexOf("[") + 1, str.indexOf("]"))
        val event = str.substring(str.indexOf("act=") + 4, str.indexOf("&url"))
        val apk = str.substring(str.indexOf("apk_sem/") + 8, str.indexOf(".apk"))
        val sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
        val transferFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val time = transferFormat.format(sourceFormat.parse(nginx_time))
        val doc = new Document
        doc.append(click_time, time).append(event_tag, event).append(apk_name, apk)
        (doc, true)
      } catch {
        case e: Exception =>
          System.err.println(new Date() + " Error: pageAdvLogAnalysis error!")
          (new Document().append("error", str), false)
      }

    })
  }

  /**
    * 取出[]里面的内容
    *
    * @param str
    * @param size
    * @return
    */
  def getContent(str: String, size: Int = 3): Array[String] = {
    var string = str
    val startFlag = "["
    val endFlag = "]"
    val result = for (i <- 0 until size) yield {
      val value = string.substring(string.indexOf(startFlag) + 1, string.indexOf(endFlag))
      string = string.substring(string.indexOf(endFlag) + 1)
      value
    }
    result.toArray
  }

  /**
    * 解析NORMAL: [2017-11-06 00:02:34] CIP[117.136.3.75] AD_CLICK[{"junhai_adid":"5798","game":"U100000289","click_time":"2017-11-06 00:02:34"}]
    * 取出前面的时间，IP，以及后面的json
    *
    * @param str
    * @return
    */
  def getAdvContent(str: String): (String, String, String) = {
    var string = str
    val startFlag = "["
    val endFlag = "]"
    try {
      val time = string.substring(string.indexOf(startFlag) + 1, string.indexOf(endFlag))
      string = string.substring(string.indexOf(endFlag) + 1)
      val ip = string.substring(string.indexOf(startFlag) + 1, string.indexOf(endFlag))
      string = string.substring(string.indexOf(endFlag) + 1)
      val json = string.substring(string.indexOf("[{\"") + 1, string.indexOf("\"}]") + 2)
      (time, ip, json)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        ("", "", "")
    }
  }


}
