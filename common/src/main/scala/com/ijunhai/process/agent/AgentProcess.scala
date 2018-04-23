package com.ijunhai.process.agent

import java.util
import java.util.{Date, Properties}

import com.ijunhai.common.http.{HttpClientUtil, Result}
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.{JunhaiLog, Monitor}
import com.ijunhai.common.offset.redisRocketMQOffset
import com.ijunhai.common.{HDFSUtil, TimeUtil}
import com.ijunhai.process.CommonProcess
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{OffsetRange, TopicQueueId}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document
import com.ijunhai.common.CleanConstants._

import scala.collection.JavaConversions._


object AgentProcess extends Serializable {
  val topicPreFix = ""
  val LOG = "log"
  val ERROR_FLAG = "Error"
  val RIGHT_FLAG = "Std"
  val OTHER_FLAG = "Other"
  var brokers = "10.13.4.84:9092,10.13.112.92:9092,10.13.18.222:9092"
  val error = "error"
  val props = new Properties()
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put("bootstrap.servers", brokers)

  def setBrokers(brokers: String): Unit = {
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    this.brokers = brokers
  }

  def dalanSDK(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink], topic: String): Unit = {
    CommonProcess.extraCoin(rdd).foreach(log => {
      var boolean = true
      val info: (Document, Int, String) =
        try {
          JunhaiLog.BsonParser(log, "DATA[")
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            kafkaSink.value.send("monitor", topic + ": json parse error:" + log)
            boolean = false
            (new Document(), 0, "")
        }
      if (boolean) {
        val serverTs = info._2
        val log = KafkaLogProcess.documentFlat("", info._1)
          .append("ip", info._3).append("server_ts", serverTs)
          .append("server_date_day", TimeUtil.time2DateString("yyyy-MM-dd", serverTs, TimeUtil.SECOND))
        kafkaSink.value.send(topic, log.toJson())
      }
    })

  }

  def sdkProcess(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink], topic: String): Unit = {

    CommonProcess.extraCoin(rdd).map(log => {
      var boolean = true
      val info: (Document, Int, String) =
        try {
          JunhaiLog.BsonParser(log, "ANALYSIS[")
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            kafkaSink.value.send("monitor", topic + ": json parse error:" + log)
            boolean = false
            (new Document(), 0, "")
        }
      if (boolean) {
        val srcDoc = info._1
        val serverTs = info._2
        val doc = srcDoc.get("device").asInstanceOf[Document].append("analysis_id", srcDoc.getString("analysis_id"))
          .append("server_ts", serverTs).append("server_date_day", TimeUtil.time2DateString("yyyy-MM-dd", serverTs, TimeUtil.SECOND))
          .append("ip", info._3)
        val finalDoc: util.ArrayList[Document] = srcDoc.get("event_record") match {
          case arrayList: util.ArrayList[Document] =>
            arrayList.foreach(log => {
              for (k <- doc.keySet()) {
                log.append(k, JunhaiLog.getString(doc, k))
              }
            })
            arrayList
          case document: Document =>
            for (k <- doc.keySet()) {
              document.append(k, JunhaiLog.getString(doc, k))
            }
            new util.ArrayList[Document]() {
              add(document)
            }
        }

        (finalDoc, boolean)
      } else {
        (new util.ArrayList[Document](), boolean)
      }
    }).filter(_._2).flatMap(_._1).foreach(log => {
      kafkaSink.value.send(topic, log.toJson())
    })
  }

  def agentNewProcess(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink], greenPlumSink: Broadcast[GreenPlumSink], offset: java.util.Map[TopicQueueId
    , Array[OffsetRange]], nameServer: String, topics: java.util.List[String], group: String
                      , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    val accum = rdd.sparkContext.longAccumulator("longAccum")
    if (!rdd.isEmpty()) {
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      val msgs = CommonProcess.extraData(rdd, kafkaSink, nameServer, topicsStr)
      val firstStr = "NORMAL: ["
      val secondStr = "] CIP["
      val beforeAccum = accum.value
      msgs.foreach(tuple => {
        val msg = tuple._1
        val time = msg.substring(msg.indexOf(firstStr) + firstStr.length, msg.indexOf(secondStr)).split(" ")(0)
        val topic = if (tuple._2 == null) "mqPullError" else tuple._2
        val doc = new Document().append("server_date_day", time).append("msg", msg)
        accum.add(1L)
        kafkaSink.value.send(topic, doc.toJson())
      })
      val a = accum.value - beforeAccum
      kafkaSink.value.send("monitor", "Accum:" + a)
      if (offset != null) {
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }

  def logProcess(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink], greenPlumSink: Broadcast[GreenPlumSink], offset: java.util.Map[TopicQueueId
    , Array[OffsetRange]], nameServer: String, topics: java.util.List[String], group: String
                 , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    val time = new Date().getTime
    if (!rdd.isEmpty()) {
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      val msgs = CommonProcess.extra(rdd, greenPlumSink, nameServer, topicsStr)
      val result = msgs.mapPartitions(it => {
        it.map(msg => {
          val topic = if (msg._2 == null) "mqPullError" else msg._2
          kafkaSink.value.send(topic, msg._1)
        })
      })
      //GPSink: Broadcast[GreenPlumSink],dataSource: String,serverName: String,metricName: String,value: Int ,
      // type: String,timestamp: Long,fileName: String = "",srcOrDec: String = "",tag: String = ""
      Save2GP.metricMonitor(greenPlumSink, topics.get(0), nameServer, Monitor.sendToKafka, result.count().toInt, Monitor.logFile, time)

      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }

  def processSink(rdd: RDD[MessageExt], kafkaSink: Broadcast[KafkaSink]
                  , offset: java.util.Map[TopicQueueId, Array[OffsetRange]]
                  , nameServer: String
                  , topics: java.util.List[String], group: String
                  , redisSinkCluster: Broadcast[RedisSink]): Unit = {
    println("------------" + System.currentTimeMillis() + "------------")
    if (!rdd.isEmpty()) {
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      val msgs = CommonProcess.extraData(rdd, kafkaSink, nameServer, topicsStr)
      val result = msgs.mapPartitions(it => {
        //  val producer = new KafkaProducer(props, null, new org.apache.kafka.common.serialization.StringSerializer)
        //  val kafkaProducer = kafka_producer.getInstance("192.168.106.128:9092")
        it.map(msg => {
          val topic = if (msg._2 == null) "mqPullError" else msg._2
          kafkaSink.value.send(topic, msg._1)
        })
      })
      Save2Kafka.saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), nameServer + ":com.ijunhai.kafka.mq2kafka.Mq2Kafka", JunhaiLog.INFO, "topic(s) is" + topicsStr + ",Total send to kafka topic", "Num", result.count() + "")
      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }

  /**
    * 独代订单
    */
  def orderJoin(rdd: RDD[MessageExt]
                , kafkaSink: Broadcast[KafkaSink]
                , gpSink: Broadcast[GreenPlumSink]
                , column: String
                , topic: String
                , hdfsCachePath: String
                , offset: java.util.Map[TopicQueueId, Array[OffsetRange]]
                , nameServer: String
                , topics: java.util.List[String]
                , group: String
                , redisSinkCluster: Broadcast[RedisSink]
                , ipPath: String
               ): Unit = {

    println("------------" + System.currentTimeMillis() + "------------")
    if (!rdd.isEmpty()) {
      val rows = "rows"
      val table = "table"
      val data = "data"
      val order_sn = "order_sn"
      val database = "database"
      val actionType = "type"
      val agent_order = "agent_order"
      val agent_order_copy = "agent_order_copy"
      val game_id = "game_id"
      val game_channel_id = "game_channel_id"
      val channel_id = "channel_id"
      val sub_pf = "sub_pf"

      val agcTuple = getAgentGameChannel(hdfsCachePath)
      val ggMap = agcTuple._1 //game_id + game_channel_id 获得 channel_id
      val gcMap = agcTuple._2 //game_id + channel_id 获得 game_channel_id

      val scMap = getSubChannel(hdfsCachePath)
      val bscMap = getAllBySubChannel(hdfsCachePath)
      val gcmMap = getAgentGame(hdfsCachePath)

      val bBytes: Broadcast[Array[Byte]] = rdd.sparkContext.broadcast(IPAddress.init(ipPath))

      val bAcMap = rdd.sparkContext.broadcast(getAgentChannel(hdfsCachePath))
      val bGgMap = rdd.sparkContext.broadcast(ggMap)
      val bGcMap = rdd.sparkContext.broadcast(gcMap)
      val bScMap = rdd.sparkContext.broadcast(scMap)
      val bBscMap = rdd.sparkContext.broadcast(bscMap)
      val bGcmMap = rdd.sparkContext.broadcast(gcmMap)
      val filterRule: Broadcast[Map[String, (util.ArrayList[String], util.ArrayList[Document])]] = rdd.sparkContext.broadcast(AgentProcess.getFilterRule(hdfsCachePath))

      val iOS128 = getIOS128(hdfsCachePath)
      val bIOS128 = rdd.sparkContext.broadcast(iOS128)
      println("agent game channel size is " + ggMap.size + ".")
      println("agent game channel size is " + gcMap.size + ".")
      var topicsStr = ""

      topics.foreach(topic => {
        topicsStr += topic + " "
      })

      val msgs: RDD[(String, String)] = CommonProcess.extraData(rdd, kafkaSink, nameServer, topicsStr)

      val document: RDD[(Document, Boolean)] = msgs.map(line => {
        val msg: String = line._1
        try {
          val doc = Document.parse(msg)
          (doc, true)
        } catch {
          case _: Exception =>
            Save2GP.errorMonitor(gpSink, topic, "json parse error", msg)
            System.err.println(new Date() + " Error: json parse error!")
            (new Document(LOG, msg), false)
        }
      })

      val flat: RDD[(Document, String, String, String)] = document.filter(_._2).flatMap(line => {
        line._1.get(rows).asInstanceOf[util.ArrayList[Document]].map(doc => {
          (doc.get(data).asInstanceOf[Document], doc.getString(database), doc.getString(table), doc.getString(actionType))
        })
      })

      //      val copyTable=flat.filter(_._3==agent_order_copy).map(line=>{
      //        (line._1.getLong(order_sn),line._1.getString(create_ip))
      //      }).collect().toMap
      //      val bCopyMap=rdd.sparkContext.broadcast(copyTable)

      val copyTable: RDD[(Document, String, String, String)] = flat.filter(_._3 == agent_order_copy)
      //添加ip和转换时间戳为年月日最后发往kafka
      val CopyResult = copyTable.map(document => {
        val order_sn = JunhaiLog.getString(document._1, "order_sn")
        document._1.remove("order_sn")
        val server_date_day = TimeUtil.time2DateString("yyyy-MM-dd", JunhaiLog.getTimestamp(document._1, "create_time"), 1)
        val ipClass: IP = new IP
        ipClass.load(bBytes.value)
        try {
          val geo: util.HashMap[String, String] = IPAddress.getIPAddress(document._1.getString("create_ip"), ipClass)
          for (key <- geo.keySet()) {
            var value: String = geo.get(key)
            if (value.contains("\"")) {
              value = value.replaceAll("\"", "")
            }
            document._1.append(key, value)
          }
        } catch {
          case e: Exception =>
            document._1.append("province", "")
            document._1.append("country", "")
            document._1.append("city", "")
        }
        document._1.append("server_date_day", server_date_day)
        document._1.append("order_sn", order_sn)
        document._1
      })
      Save2Kafka.saveDStreamAsync(CopyResult.map(_.toJson()), kafkaSink, topic + "Copy")


      val resultDocuments = flat.filter(_._3 == agent_order).map((order: (Document, String, String, String)) => {
        val orderSn = order._1.getLong(order_sn)
        //        if(bCopyMap.value.nonEmpty){
        //          order._1.append(create_ip,bCopyMap.value.get(orderSn,"0.0.0.0"))
        //        }
        //开始关联
        val gameId = order._1.getInteger(game_id)
        val columnValue = order._1.getInteger(column) + "" //默认的独代的channel_id字段
        if (gameId >= 63) {
          //此时order表中的channel_id存放的是game_channel_id的值 需要得到channel_id
          val value = bGgMap.value.getOrElse(gameId + columnValue, error) //bGgMap 独代game_id + game_channel_id 获得 channel_id
          val subPf = bAcMap.value.getOrElse(value, error) //channelId获得subpf
          order._1.put(channel_id, value)
          order._1.put(game_channel_id, columnValue) //将原来的字段名字改回来
          order._1.put(sub_pf, subPf)
        } else {
          if (!bScMap.value.contains(columnValue)) {
            //此时order表中的channel_id就是channel_id
            val value = bGcMap.value.getOrElse(gameId + columnValue, error) //拿到game_channel_id
            val subPf = bAcMap.value.getOrElse(columnValue, error)
            order._1.put(game_channel_id, value)
            order._1.put(sub_pf, subPf)
          } else {
            val value = bBscMap.value.getOrElse(columnValue, error) //根据sub_channel拿game_channel_id
            val channelId = bScMap.value.getOrElse(columnValue, error)
            order._1.put(channel_id, channelId)
            if (value.equals(error)) {
              val agentValue = bGcMap.value.getOrElse(gameId + channelId, error)
              order._1.put(JunhaiLog.game_channel_id, agentValue)
            } else {
              order._1.put(JunhaiLog.game_channel_id, value)
            }
            val subPf = bAcMap.value.getOrElse(channelId, error)
            order._1.put(sub_pf, subPf)
          }
        }
        order._1.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId + "", error)) //公司ID
        order._1.put(order_sn, orderSn + "")

        order._1
      }).cache()

      val agentWithoutHW = resultDocuments.filter {
        bson => {
          //剔除海外渠道的数据
          val channelId = bson.get(JunhaiLog.channel_id)
          channelId match {
            case i: Integer =>
              bson.put(JunhaiLog.channel_id, channelId.toString)
            case i: String =>
          }
          !JunhaiLog.haiwaiChannelId.contains(bson.getString(JunhaiLog.channel_id))
        }
      }
      //      Save2Kafka.saveDStreamSync(agentWithDalan.map(_.toJson()), kafkaSink, "filterData", 8)

      val withNewUserFlag = Save2Redis.saveDatabaseOrder(agentWithoutHW, "agent", redisSinkCluster)
        .map(document => {
          (KafkaLogProcess.documentFlat("", document, topic), JunhaiLog.getString(document, JunhaiLog.game_channel_id))
        }).cache()


      val finalDoc: RDD[(Document, String)] = withNewUserFlag.map((log: (Document, String)) => {
        var isTest = true //非测试订单
        val gameId = JunhaiLog.getString(log._1, "game_id")
        if (filterRule.value.containsKey(gameId)) {
          val server = JunhaiLog.getString(log._1, "server")
          val (listA, listB) = filterRule.value.get(gameId).asInstanceOf[(util.ArrayList[String], util.ArrayList[Document])]
          if (listA.contains(server)) {
            isTest = false
          } else if (!listB.isEmpty) {
            for (doc <- listB) {
              val start = JunhaiLog.getInt(doc, "start")
              val end = JunhaiLog.getInt(doc, "end")
              if (server.toInt >= start && server.toInt <= end) {
                isTest = false
              }
            }
          }
        }
        if (!isTest) {
          kafkaSink.value.send(topic + "Test", log._1.toJson)
        }
        (log._1, log._2, isTest)
      }).filter(_._3).map(log => (log._1, log._2))


      //      Broadcast[Map[String, (util.ArrayList[String], util.ArrayList[Document])]]

      //      Save2Kafka.saveDStreamAsync(withNewUserFlag.filter(line => {
      //        bIOS128.value.contains(line._2)
      //      }).map(_._1).map(_.toJson()), kafkaSink, topic + OTHER_FLAG)
      //
      //      Save2Kafka.saveDStreamAsync(withNewUserFlag.filter(line => {
      //        !bIOS128.value.contains(line._2)
      //      }).map(_._1).map(_.toJson()), kafkaSink, topic)

      Save2Kafka.save(finalDoc.filter(line => {
        bIOS128.value.contains(line._2)
      }).map(_._1).map(_.toJson()), kafkaSink, gpSink, topic + OTHER_FLAG, nameServer)

      Save2Kafka.save(finalDoc.filter(line => {
        !bIOS128.value.contains(line._2)
      }).map(_._1).map(_.toJson()), kafkaSink, gpSink, topic, nameServer)

      withNewUserFlag.unpersist()
      resultDocuments.unpersist()


      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }

  val FORMAT_STR = "yyyy-MM-dd HH:mm:ss"

  /**
    * 订单关联处理
    *
    * @param rdd           rocketMQ未经处理的数据
    * @param column        order表中需要关联的字段
    * @param topic         写到kafka的topic
    * @param hdfsCachePath 在hdfs上的文件缓存目录
    * @param group         消费组，用来记录消费的分组
    * @param system        //指定处理的是大蓝的还是君海联运的还是神奇的还是海外的
    */
  def JunhaiChannelDatabase(rdd: RDD[MessageExt]
                            , kafkaSink: Broadcast[KafkaSink]
                            , gpSink: Broadcast[GreenPlumSink]
                            , column: String
                            , topic: String //desc
                            , hdfsCachePath: String
                            , offset: java.util.Map[TopicQueueId, Array[OffsetRange]]
                            , nameServer: String
                            , topics: java.util.List[String] //src
                            , group: String
                            , system: String
                            , redisSinkCluster: Broadcast[RedisSink]
                           ): Unit = {
    val ORDER_JH = "order_jh"
    val CHANNEL_OTHER_TABLES = "Others"
    val ORDER = "order"
    val USER = "user"
    println("------------" + System.currentTimeMillis() + "------------")
    if (!rdd.isEmpty()) {
      val rows = "rows"
      val table = "table"
      val data = "data"
      val game_id = "game_id"
      val jh_app_id = "jh_app_id"
      val gcgTuple = AgentProcess.getAgentGameChannelForChannel(hdfsCachePath)
      val bGcgMap = rdd.sparkContext.broadcast(gcgTuple)
      val gameChannel = AgentProcess.getAgent2GameChannel(hdfsCachePath, system)
      val bGameChannel = rdd.sparkContext.broadcast(gameChannel)
      val (rmb, usd) = AgentProcess.getRate(hdfsCachePath)
      val bRmb = rdd.sparkContext.broadcast(rmb)
      val bUsd = rdd.sparkContext.broadcast(usd)
      val gcmMap = AgentProcess.getAgentGame(hdfsCachePath)
      val bGcmMap = rdd.sparkContext.broadcast(gcmMap)
      val bIOS128 = rdd.sparkContext.broadcast(getIOS128(hdfsCachePath))
      var topicsStr = ""
      topics.foreach(topic => {
        topicsStr += topic + " "
      })
      val source1 = topicsStr.split(" ")(0)

      val msgs: RDD[(String, String)] = CommonProcess.extra(rdd, gpSink, nameServer, topicsStr)

      val document = msgs.map {
        case (msg, _) =>
          try {
            val doc = Document.parse(msg)
            (doc, true)
          } catch {
            case e: Exception =>
              Save2GP.errorMonitor(gpSink, topic, "json parse error", msg)
              (new Document(LOG, msg), false)
          }
      }
      //      Save2Kafka.saveDStreamAsync(document.filter(!_._2).map(_._1.toJson()), kafkaSink, topic + ERROR_FLAG, nameServer + ":com.ijunhai.kafka.mq2kafka.JunhaiChannelDatabase")

      val filterDocuments = document.filter(_._2).flatMap(line => {
        line._1.get(rows).asInstanceOf[util.ArrayList[Document]].map(doc => {
          val tableName = doc.getString(table)
          (doc.get(data).asInstanceOf[Document], tableName)
        })
      })

      val orderData = filterDocuments.filter {
        case (_, tableName) =>
          tableName == ORDER
      }
      val userData = filterDocuments.filter {
        case (_, tableName) =>
          tableName == USER
      }
      val order_jhData = filterDocuments.filter {
        case (_, tableName) =>
          tableName == ORDER_JH
      }
      var otherTableData = filterDocuments.filter {
        case (_, tableName) =>
          !(tableName == ORDER || tableName == USER || tableName == ORDER_JH)
      }.map(_._1)

      val orderDocument: RDD[Document] = system match {
        case "haiwai" =>
          orderTransform(orderData.map(_._1), system, game_id, bGcgMap, bGameChannel, bGcmMap).map(line => {
            val document = line
            val order = JunhaiLog.getDocument(document, JunhaiLog.order)
            val currencyAmount = JunhaiLog.getDouble(order, JunhaiLog.currency_amount)
            val currencyType = JunhaiLog.getString(order, JunhaiLog.currency_type)

            val rmb = bRmb.value.getOrDefault(currencyType, 1.0) * currencyAmount
            val usd = bUsd.value.getOrDefault(currencyType, 1.0) * currencyAmount
            order.append(JunhaiLog.cny_amount, rmb)
            order.append(JunhaiLog.usd_amount, usd)

            document
          })
        case "dalan" =>
          val orderTmp = orderTransform(orderData.map(_._1), system, game_id, bGcgMap, bGameChannel, bGcmMap)

          Save2Kafka.save(orderTmp.filter { log => {
            !bIOS128.value.contains(JunhaiLog.getSecondColumnString(log, JunhaiLog.agent, JunhaiLog.game_channel_id))
          }
          }.map(_.toJson()), kafkaSink, gpSink, source1 + CHANNEL_OTHER_TABLES, nameServer) //dlUserOrderOther

          orderTmp.filter { log => { //留下大蓝小于128的iOS独代数据
            bIOS128.value.contains(JunhaiLog.getSecondColumnString(log, JunhaiLog.agent, JunhaiLog.game_channel_id))
          }
          }

        case _ =>
          orderTransform(orderData.map(_._1), system, game_id, bGcgMap, bGameChannel, bGcmMap)
      }

      val resultDocuments = if (system.equals(JunhaiLog.DALAN)) {
        otherTableData = otherTableData.union(registerTransform(userData.map(_._1), jh_app_id, bGameChannel, bGcmMap))
        orderDocument
      } else {
        orderDocument.union(registerTransform(userData.map(_._1), jh_app_id, bGameChannel, bGcmMap)).cache()
      }

      val order_jh = orderTransform(order_jhData.map(_._1), system, game_id, bGcgMap, bGameChannel, bGcmMap, false) //最后一个标识为false表示使用jh_channel来关联。

      val withNewUserFlag = Save2Redis.saveDStream(resultDocuments, system, redisSinkCluster)
        .map(document => {
          (KafkaLogProcess.documentFlat("", document, source1), JunhaiLog.getSecondColumnString(document, JunhaiLog.agent, JunhaiLog.game_channel_id))
        })

      val orderWithNewUserFlag = Save2Redis.saveDStream(order_jh, system, redisSinkCluster)
        .map(document => {
          (KafkaLogProcess.documentFlat("", document, source1), JunhaiLog.getSecondColumnString(document, JunhaiLog.agent, JunhaiLog.game_channel_id))
        })

      Save2Kafka.save(withNewUserFlag.map(_._1).map(_.toJson()), kafkaSink, gpSink, topic, nameServer)
      Save2Kafka.save(orderWithNewUserFlag.map(_._1).map(_.toJson()), kafkaSink, gpSink, ORDER_JH, nameServer) //ORDER_JH 绕过支付 只有大蓝有
      Save2Kafka.save(otherTableData.map(_.toJson()), kafkaSink, gpSink, source1 + CHANNEL_OTHER_TABLES, nameServer) //hwUserOrderOthers

      resultDocuments.unpersist()

      if (offset != null) {
        println("update offset")
        topics.toArray().foreach(topic => {
          redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group, redisSinkCluster)
        })
      }
    }
  }

  def orderTransform(rdd: RDD[Document]
                     , system: String
                     , column: String
                     , bGameChannelIdMap: Broadcast[Map[String, (String, String)]]
                     , bGameChannel: Broadcast[Map[String, (String, String, String)]]
                     , bGcmMap: Broadcast[Map[String, String]]
                     , combineFlag: Boolean = true): RDD[Document] = {
    val bAcMap = rdd.sparkContext.broadcast(getAgentChannel(hdfsCachePath))
    //{"id":"5367","order_sn":"2016030910053901456","user_id":"10996","money":"50.00",
    // "money_type":"TWD","type":"1","status":"4","pay_status":"1","game_id":"100000000",
    // "jh_channel":"google_play","device_id":"ffffffff-bac0-82f9-ffff-ffffe497cd53",
    // "server":"8","pay_type":"1","cp_trade_sn":"14575370812029",
    // "payment_trade_sn":"GPA.1388-3220-4154-60452","goods_id":"2","goods_name":"\u947d\u77f3",
    // "exchange_rate":"100","game_role_id":"2029","game_role_name":"\u8461\u8404kai",
    // "game_url":"http:\/\/twapi.qjxy.ijunhai.com\/tw_pay.php","ext1":"","ext2":"",
    // "create_time":"2016-03-09 23:24:09","update_time":"2016-03-09 23:24:20"}

    rdd.filter(doc => doc.get(column) != null).map(doc => {
      //order表中不存在jh_app_key user表中不存在game_id
      val junhaiLog = new JunhaiLog()

      if (combineFlag) { //区分order_jh和order
        //开始关联
        val columnValue = JunhaiLog.getString(doc, column) //默认的独代的channel_id字段
      val values = bGameChannel.value.getOrDefault(columnValue, (error, error, error))
        val gameChannelId = values._1
        val channelId = values._2
        val gameId = values._3
        junhaiLog.game_channel_id = gameChannelId
        junhaiLog.channel_id = channelId
        junhaiLog.game_id = gameId
      } else {
        val columnValue = JunhaiLog.getString(doc, "jh_channel") //默认的独代的channel_id字段 order_jh中使用jh_channel字段
        val values = bGameChannelIdMap.value.getOrDefault(columnValue, (error, error))
        val gameChannelId = columnValue
        val channelId = values._1
        val gameId = values._2
        junhaiLog.game_channel_id = gameChannelId
        junhaiLog.channel_id = channelId
        junhaiLog.game_id = gameId
      }

      junhaiLog.os_type = bAcMap.value.getOrElse(junhaiLog.channel_id, "") //根据channelId获得subpf
      junhaiLog.order_sn = JunhaiLog.getString(doc, JunhaiLog.order_sn)
      junhaiLog.user_id = JunhaiLog.getString(doc, JunhaiLog.user_id)
      junhaiLog.currency_amount = JunhaiLog.getDouble(doc, "money")
      junhaiLog.currency_type = JunhaiLog.getString(doc, "money_type")
//      if (system != JunhaiLog.DALAN) {
        val status = JunhaiLog.getInt(doc, "status")
        val newStatus = status match {
          case 1 => //已下单
            "create_order"
          case 2 => //已付费
            "paid_success"
          case 3 => //通知CP失败
            "notify_cp_failue"
          case 4 => //通知CP成功
            "notify_cp_success"
          case 5 => //CP处理完成
            "finish"
          case _ =>
            status + ""
        }
        junhaiLog.order_step = newStatus
        val payStatus = JunhaiLog.getInt(doc, "pay_status")
        val newPayStatus = payStatus match {
          case 0 => //失败
            "failure"
          case 1 => //成功
            "success"
          case _ =>
            payStatus + ""
        }
        junhaiLog.order_status = newPayStatus
//      }
      //jh_channel
      junhaiLog.device_id = JunhaiLog.getString(doc, JunhaiLog.device_id)
      junhaiLog.role_server = JunhaiLog.getString(doc, "server")
      val payType = JunhaiLog.getIntString(doc, "pay_type")
      val newPayType = if (system == JunhaiLog.HAIWAI) {
        payType match {
          case "1" =>
            "google_pay"
          case "2" =>
            "mycard"
          case "3" =>
            "visa_global"
          case "4" =>
            "apple_pay"
          case "5" =>
            "mol"
          case "6" =>
            "huawei"
          case "7" =>
            "onestore"
          case _ =>
            payType
        }
      } else {
        payType match {
          case "1" | "9" | "10" | "14" => //支付宝
            "alipay"
          case "6" | "12" | "13" | "15" | "17" | "18" | "19" | "21" | "22" => //微信
            "weichat_pay"
          case "5" => //易宝
            "yeebao"
          case "7" => //苹果
            "apple_pay"
          case "8" => //爱贝
            "iapppay"
          case "20" => //银联
            "unionpay"
          case _ =>
            payType
        }
      }

      junhaiLog.order_type = newPayType
      junhaiLog.cp_trade_sn = if (doc.getString(JunhaiLog.cp_trade_sn) == null) "" else doc.getString(JunhaiLog.cp_trade_sn)
      junhaiLog.channel_trade_sn = if (doc.getString("payment_trade_sn") == null) "" else doc.getString("payment_trade_sn")
      junhaiLog.role_id = if (doc.getString("game_role_id") == null) "" else doc.getString("game_role_id")
      junhaiLog.role_name = if (doc.getString("game_role_name") == null) "" else doc.getString("game_role_name")
      junhaiLog.server_ts = JunhaiLog.getTimestamp(doc, "update_time", FORMAT_STR)
      junhaiLog.client_ip = JunhaiLog.getString(doc, "create_ip")
      junhaiLog.game_ver = JunhaiLog.getString(doc, "application_version")
      junhaiLog.sdk_ver = JunhaiLog.getString(doc, "sdk_version")
      junhaiLog.android_imei = JunhaiLog.getString(doc, "imei")
      //        com.ijunhai.util.TimeUtil.dateString2Time(FORMAT_STR,doc.getString("update_time"),com.ijunhai.util.TimeUtil.SECOND).toInt
      junhaiLog.event = "order"
      val result = JunhaiLog.getOrder(junhaiLog) //构成整条数据

      if (system == JunhaiLog.DALAN) {  //dalan要和独代合并
        result.append("pay_status", payStatus)
          .append("status", status)
          .append("sub_pf", junhaiLog.os_type)
      }

      result.get(JunhaiLog.game).asInstanceOf[Document].append(JunhaiLog.APP_KEY, JunhaiLog.getString(doc, column))
      result.get(JunhaiLog.game).asInstanceOf[Document].append(JunhaiLog.company_id,
        bGcmMap.value.getOrDefault(junhaiLog.game_id, error))
      result
    })
  }

  def registerTransform(rdd: RDD[Document]
                        , column: String
                        , bGameChannel: Broadcast[Map[String, (String, String, String)]]
                        , bGcmMap: Broadcast[Map[String, String]]): RDD[Document] = {

    rdd.filter(doc => doc.get(column) != null).map(doc => {
      //order表中不存在jh_app_key user表中不存在game_id
      val junhaiLog = new JunhaiLog()
      //开始关联
      val columnValue = JunhaiLog.getString(doc, column) //默认的独代的channel_id字段

      val values = bGameChannel.value.getOrDefault(columnValue, (error, error, error))
      val gameChannelId = values._1
      val channelId = values._2
      val gameId = values._3
      junhaiLog.game_channel_id = gameChannelId
      junhaiLog.channel_id = channelId
      junhaiLog.game_id = gameId

      junhaiLog.user_id = JunhaiLog.getString(doc, JunhaiLog.user_id)
      //jh_channel
      junhaiLog.device_id = JunhaiLog.getString(doc, JunhaiLog.device_id)
      junhaiLog.server_ts = JunhaiLog.getTimestamp(doc, "register_time", FORMAT_STR)
      junhaiLog.os_ver = JunhaiLog.getString(doc, "system_version")
      junhaiLog.device_name = JunhaiLog.getString(doc, "system_name")
      junhaiLog.phone_num = JunhaiLog.getString(doc, "tel_num")
      junhaiLog.email_num = JunhaiLog.getString(doc, "email")
      junhaiLog.login_type = JunhaiLog.getString(doc, "reg_type")
      val sex = JunhaiLog.getString(doc, "sex")
      junhaiLog.gender = sex match {
        case "1" =>
          "male"
        case "2" =>
          "female"
        case _ =>
          sex
      }
      junhaiLog.birth = JunhaiLog.getString(doc, "birth")
      junhaiLog.android_imei = JunhaiLog.getString(doc, "imei")
      junhaiLog.event = "register"

      val result = JunhaiLog.getRegister(junhaiLog)
      result.get(JunhaiLog.game).asInstanceOf[Document].append(JunhaiLog.APP_KEY, JunhaiLog.getString(doc, column))
      result.get(JunhaiLog.game).asInstanceOf[Document].append(JunhaiLog.company_id,
        bGcmMap.value.getOrDefault(junhaiLog.game_id, error))
      result
    })
  }


  /**
    * @return (game_id,pack_name)
    */
  def getPackName(): Map[String, String] = {
    val game_id = "game_id"
    val package_name = "package_name"
    val url = "http://bigdata.ijunhai.com/api/getPackageName"
    val content = "content"
    val voidedPurchases = "voidedPurchases"
    //    val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = geturlContent(url)

    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Document.parse(geturlContent(url))
    }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val game_pack = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(game_id), doc.getString(package_name))
    }).map(line => (line._2, line._1)).toMap
    game_pack
  }

  /**
    * 通过game_channel_id来获取包相关信息
    *
    * @return
    */
  def getReference(sparkContext: SparkContext, channel_id: String, b_map: Broadcast[Map[String, Document]]): Document = {
    b_map.value.getOrElse(channel_id, new Document())
  }

  /**
    * 形成两个map结构，通过game_channel_id拿channel_id或者通过channel_id拿game_channel_id
    */
  def getAgentGameChannel(hdfsCachePath: String, tag: Int = 0): (Map[String, String], Map[String, String]) = {
    val game_id = "game_id"
    val channel_id = "channel_id"
    val game_channel_id = "game_channel_id"
    val fileName = "game_channel_id.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentGameChannel"
    val content = "content"

    //    val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc =
      try {
        Document.parse(contentStr)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          println("get from url")
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val ggMap = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      if (tag == 1) {
        (doc.getString(game_channel_id), doc.getString(channel_id) + "|" + doc.getString(game_id))
      } else {
        (doc.getString(game_id) + doc.getString(game_channel_id), doc.getString(channel_id))
      }
    }).toMap //game_id+game_channel_id 获得 channel_id

    val gcMap = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(game_id) + doc.getString(channel_id), doc.getString(game_channel_id))
    }).toMap
    (ggMap, gcMap)
  }

  /**
    * 通过game_id获取游戏公司id
    */
  def getAgentGame(hdfsCachePath: String): Map[String, String] = {
    val game_id = "game_id"
    val company_id = "company_id"
    val fileName = "agent_game.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentGame"
    val content = "content"

    //    val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc =
      try {
        Document.parse(contentStr)
      } catch {
        //hdfs拿不到则去接口拿
        case e: Exception =>
          e.printStackTrace()
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val gcMap = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(game_id), doc.getString(company_id))
    }).toMap
    gcMap
  }

  /**
    * 形成两个map结构，通过game_channel_id拿channel_id或者通过channel_id拿game_channel_id
    */
  def getAgentGameChannelForChannel(hdfsCachePath: String): Map[String, (String, String)] = {
    val game_id = "game_id"
    val channel_id = "channel_id"
    val game_channel_id = "game_channel_id"
    val fileName = "game_channel_id.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentGameChannel"
    val content = "content"

    //    val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc =
      try {
        Document.parse(contentStr)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val ggMap = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(game_channel_id), (doc.getString(channel_id), doc.getString(game_id)))
    }).toMap
    ggMap
  }

  /**
    * sub_pf是操作系统类型，任何情况下都应该通过渠道ID力气关联sub_pf
    */
  def getAgentChannel(hdfsCachePath: String): Map[String, String] = {
    val pf = "pf"
    val channel_id = "channel_id"
    val fileName = "agent_channel.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentChannel"
    val content = "content"

    //    val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc =
      try {
        Document.parse(contentStr)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val map = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(channel_id), doc.getString(pf))
    }).toMap
    map
  }

  /**
    * 通过sub_channel_id拿到channel_id
    */
  def getSubChannel(hdfsCachePath: String): Map[String, String] = {
    val sub_channel_id = "sub_channel_id"
    val channel_id = "channel_id"
    val fileName = "sub_channel.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentSubChannel"
    val content = "content"

    //    val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc =
      try {
        Document.parse(contentStr)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val map = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(sub_channel_id), doc.getString(channel_id))
    }).toMap
    map
  }

  /**
    * 通过game_id+sub_channel_id来获得game_channel_id
    */
  def getAllBySubChannel(hdfsCachePath: String): Map[String, String] = {
    val channel_id = "channel_id" //名字叫channel_id实际放的是sub_channel_id的值，所以要当做sub_channel_id来用
    val game_channel_id = "game_channel_id"
    val fileName = "agent_old_game_channel.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentOldGameChannel"
    val content = "content"

    //    val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc =
      try {
        Document.parse(contentStr)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val map = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(channel_id), doc.getString(game_channel_id))
    }).toMap
    map
  }

  //  /**
  //    * 君海自有渠道不走独代的数据接口获取(被getAgent2GameChannel替换)
  //    *
  //    * @param hdfsCachePath
  //    * @return
  //    */
  //  def getAgentBelongGameChannel(hdfsCachePath:String): (Map[String,(String,String,String)],Map[String,(String,String)]) ={
  //    val channel_id="channel_id" //名字叫channel_id实际放的是sub_channel_id的值，所以要当做sub_channel_id来用
  //    val belong_game_id="belong_game_id"
  //    val game_channel_id="game_channel_id"
  //    val game_id="game_id"
  //    val fileName="agent_2_game_channel.json"
  //    val url="http://bigdata.ijunhai.com/api/getBelongGameToGameChannelId"
  //    val content="content"
  //
  //    val contentStr=getContent(url,hdfsCachePath+fileName)
  ////    val contentStr=getContent(hdfsCachePath+fileName)
  //
  //    val resultDoc=
  //      try {
  //        Document.parse(contentStr)
  //      }catch {
  //        case e:Exception=>{
  //          e.printStackTrace()
  //          Document.parse(getContent(url,hdfsCachePath+fileName))
  //        }
  //      }
  //    val referenceDocuments=resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
  //    val belong_game_idMap=referenceDocuments.toArray.map(document=>{
  //      val doc=document.asInstanceOf[Document]
  //      (doc.getString(belong_game_id),(doc.getString(game_channel_id),doc.getString(channel_id),doc.getString(game_id)))
  //    }).toMap
  //    val game_channel_idMap=referenceDocuments.toArray.map(document=>{
  //      val doc=document.asInstanceOf[Document]
  //      (doc.getString(game_channel_id),(doc.getString(channel_id),doc.getString(game_id)))
  //    }).toMap
  //    (belong_game_idMap,game_channel_idMap)
  //  }

  /**
    * 君海自有渠道  走独代的数据接口获取
    */
  def getAgent2GameChannel(hdfsCachePath: String, service: String = ""): Map[String, (String, String, String)] = {
    val channel_id = "channel_id" //名字叫channel_id实际放的是sub_channel_id的值，所以要当做sub_channel_id来用
    val game_channel_id = "game_channel_id"
    val belong_game_id = "belong_game_id"
    val game_id = "game_id"
    val fileName = "agent_2_game_channel.json"
    val url = "http://bigdata.ijunhai.com/api/getBelongGameToGameChannelId"
    val content = "content"

    //        val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Document.parse(getContent(url, hdfsCachePath + fileName))
    }
    val referenceDocuments: util.ArrayList[Document] = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]


    val map = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      if (service.equals("haiwai")) {
        if (doc.getString("sub_pf").equals("4") || doc.getString("sub_pf").equals("5")) {
          //海外的belong_game_id会对应多个值，要根据sub_pf确定唯一
          (doc.getString(belong_game_id), (doc.getString(game_channel_id), doc.getString(channel_id), doc.getString(game_id)))
        } else {
          ("", ("", "", ""))
        }
      } else {
        (doc.getString(belong_game_id), (doc.getString(game_channel_id), doc.getString(channel_id), doc.getString(game_id)))
      }
    }).toMap
    map
  }

  /**
    * 君海自有渠道  走独代的数据接口获取
    */
  def getIOS128(hdfsCachePath: String): Set[String] = {

    val game_channel_id = "game_channel_id"
    val fileName = "IOS128.json"
    val url = "http://bigdata.ijunhai.com/api/getIOS128" //小于128ios独代
    val content = "content"

    //        val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Document.parse(getContent(url, hdfsCachePath + fileName))
    }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val set = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      doc.getString(game_channel_id)
    }).toSet
    set
  }

  /**
    * 君海自有渠道  走独代的数据接口获取
    */
  def getRate(hdfsCachePath: String): (Map[String, Double], Map[String, Double]) = {
    val from_currency = "from_currency"
    val to_currency = "to_currency"
    val exchange_rate = "exchange_rate"
    val fileName = "rate.json"
    val url = "http://bigdata.ijunhai.com/api/getRate"
    val content = "content"

    //    val contentStr=getContent(url,hdfsCachePath+fileName)
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Document.parse(getContent(url, hdfsCachePath + fileName))
    }
    val referenceDocuments = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val RMBmap = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(from_currency), doc.getString(to_currency), JunhaiLog.getDouble(doc, exchange_rate))
    }).filter(_._2 == "CNY").map(line => (line._1, line._3)).toMap
    val USDmap = referenceDocuments.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(from_currency), doc.getString(to_currency), JunhaiLog.getDouble(doc, exchange_rate))
    }).filter(_._2 == "USD").map(line => (line._1, line._3)).toMap
    (RMBmap, USDmap)
  }

  /**
    * 获取http上的请求默认连接超时时间为3000毫秒，数据传输时间为4000毫秒
    * 如果http请求超时或者错误，则去上次缓存的地方拿，目前存在hdfs上
    * 拿到的话就覆盖hdfs上的内容
    * 如果hdfs上也没有就报错
    *
    * @param url       请求的url链接
    * @param cacheFile hdfs缓存文件
    * @return
    */
  def getContent(url: String, cacheFile: String): String = {
    val hcu = new HttpClientUtil()
    val result = try {
      hcu.doGet(url)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        System.err.println(new Date + " Error: get url=" + url + "fail")
        new Result()
    }
    if (result.getStatusCode != 200) {
      System.out.println(new Date + " Error: the status code of url=" + url + " is " + result.getStatusCode)
      new String(HDFSUtil.readFromHdfs(cacheFile))
    } else {
      HDFSUtil.uploadToHdfs(cacheFile, result.getContent.getBytes)
      result.getContent
    }
  }

  def getFilterRule(hdfsCachePath: String): Map[String, (util.ArrayList[String], util.ArrayList[Document])] = {
    val game_id = "game_id"
    val fileName = "test_server.json"
    val url = "http://bigdata.ijunhai.com/api/getTestServer" //小于128ios独代
    val content = "content"
    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Document.parse(getContent(url, hdfsCachePath + fileName))
    }
    resultDoc.get(content).asInstanceOf[util.ArrayList[Document]].map(doc => {
      //1.start end
      //2.多个start end in
      //3. in
      val ruleList1 = new util.ArrayList[String]()
      val ruleList2 = new util.ArrayList[Document]()
      doc.get("test_server").asInstanceOf[util.ArrayList[Any]].map {
        case value: Document =>
          ruleList2.add(value)
        case value: String =>
          ruleList1.add(value)
      }
      (doc.getString(game_id), (ruleList1, ruleList2))
    }).toMap
  }


  /**
    * 直接从hdfs上读数据
    *
    * @param cacheFile hdfs缓存文件
    * @return
    */
  def getContent(cacheFile: String): String = {
    new String(HDFSUtil.readFromHdfs(cacheFile))
  }

  def geturlContent(url: String): String = {
    val hcu = new HttpClientUtil()
    val result = try {
      hcu.doGet(url)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        System.err.println(new Date + " Error: get url=" + url + "fail")
        new Result()
    }
    if (result.getStatusCode != 200) {
      System.out.println(new Date + " Error: the status code of url=" + url + " is " + result.getStatusCode)
      null
    } else {
      result.getContent
    }
  }
}

