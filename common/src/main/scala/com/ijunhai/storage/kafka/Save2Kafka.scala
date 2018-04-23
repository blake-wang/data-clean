package com.ijunhai.storage.kafka

import java.util.concurrent.Future
import java.util.{Date, Properties}

import com.ijunhai.common.logsystem.{JunhaiLog, Monitor}
import com.ijunhai.storage.greenplum.{GreenPlumSink, Save2GP}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.spark.broadcast.Broadcast
//import org.apache.kafka.common.errors.{AuthorizationException, OutOfOrderSequenceException, ProducerFencedException}
import org.apache.spark.rdd.RDD
import org.bson.Document

/**
  * Created by Admin on 2017-08-14.
  */
object Save2Kafka {
  var brokers = "ukafka-iezd30-kafka1:9092,ukafka-iezd30-kafka2:9092,ukafka-iezd30-kafka3:9092,ukafka-iezd30-kafka4:9092,ukafka-iezd30-kafka5:9092,ukafka-iezd30-kafka6:9092"
//  var brokers = "10.13.4.84:9092,10.13.112.92:9092,10.13.18.222:9092,10.13.177.20:9092,10.13.56.241:9092,10.13.135.12:9092"
  //  var brokers = "192.168.1.112:9092,192.168.1.110:9092,192.168.1.111:9092"
  val nameServer2Ip = Map(
    "bjc-apilog.ijunhai.net:19876" -> "106.75.104.6",
    "bjd-apilog.ijunhai.net:19876" -> "123.59.84.230",
    "txtj-apilog.ijunhai.net:19876" -> "211.159.179.169",
    "bjc-apilog.dalan.net:19876" -> "123.59.51.142",
    "alhk.hw.net:19876" -> "47.52.35.95",
    "bjc-tjlog.ijunhai.net:19876" -> "123.59.62.55"
  )
  val receivedFromMQ = "rcvdFromMQ"
  val sendToKafka = "sendToKafka"
  val receivedFromKafka = "rcvdFromKafka"
  val logFile = "LogFile"

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  def setBroker(brokers: String): Unit = {
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    this.brokers = brokers
  }

  def saveDStreamAsync(rdd: RDD[String], kafkaSink: Broadcast[KafkaSink], topic: String, className: String = "com.ijunhai.kafka.Kafka2Spark"): Unit = {
    if (!rdd.isEmpty()) {
      val result: RDD[Future[RecordMetadata]] = rdd.mapPartitions(p => {
        val temp = p.map(log => {
          kafkaSink.value.send(topic, log)
        })
        temp
      })
      saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), className, JunhaiLog.INFO, "Total send to kafka topic " + topic, "num", result.count() + "")
    }
  }

  def save(rdd: RDD[String], kafkaSink: Broadcast[KafkaSink], gpSink: Broadcast[GreenPlumSink], topic: String, nameServer: String): Unit = {
    val time = new Date().getTime
    if (!rdd.isEmpty()) {
      val result: RDD[Future[RecordMetadata]] = rdd.mapPartitions(p => {
        val temp = p.map(log => {
          kafkaSink.value.send(topic, log)
        })
        temp
      })
      Save2GP.metricMonitor(gpSink, getDataSource(topic), nameServer, Monitor.sendToKafka, result.count().toInt, Monitor.logFile, time, "", topic)
    }
  }

  def getDataSource(topicName: String): String = {
    val haiwai = "haiwai"
    val dalan = "dalan"
    val agent = "agent"
    val chumeng = "chumeng"

    val youyunorder = "YouyunOrder"
    val hwuserorder = "hwUserOrder"
    val dluserorder = "dlUserOrder"
    val order_jh = "order_jh"
    val cmuserorder = "cmUserOrder"
    val topic = topicName.toLowerCase()

    if (topic.contains(haiwai)) {
      haiwai
    } else if (topic.contains(dalan)) {
      dalan
    } else if (topic.contains(agent)) {
      agent
    } else if (topic.contains(chumeng)) {
      chumeng
    } else if (topic.contains(cmuserorder.toLowerCase())) {
      cmuserorder
    } else if (topic.contains(youyunorder.toLowerCase())) {
      youyunorder
    } else if (topic.contains(hwuserorder.toLowerCase())) {
      hwuserorder
    } else if (topic.contains(dluserorder.toLowerCase()) || topic.contains(order_jh)) {
      dluserorder
    } else {
      topicName
    }
  }


  def saveDStreamSync(rdd: RDD[String], kafkaSink: Broadcast[KafkaSink], topic: String
                      , num: Int
                      , className: String = "com.ijunhai.kafka.Kafka2Spark"
                     ): Unit = {
    //    println("Start:" + System.currentTimeMillis())
    if (!rdd.isEmpty()) {
      val result: RDD[Boolean] = rdd.repartition(num).mapPartitions(p => {
        //        lazy val producer = new KafkaProducer(props, null, new org.apache.kafka.common.serialization.StringSerializer)
        val temp = p.map(log => {
          kafkaSink.value.syncSend(topic, log)
          //          var offset = -1L
          //          while(offset<0){
          //            try {
          //              offset = producer.send(new ProducerRecord(topic, log)).get().offset()
          //            } catch {
          //              case ex: KafkaException =>
          //            }
          //          }
          //          true

        })
        temp
      })
      saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), className, JunhaiLog.INFO, "Total send to kafka topic " + topic, "num", result.count() + "")
    }
  }

  //  def saveDStreamTransaction(rdd: RDD[String], topic: String, topics: java.util.List[String] , offset: java.util.Map[TopicQueueId, Array[OffsetRange]], nameServer: String, group: String): Unit = {
  //    //    println("Start:" + System.currentTimeMillis())
  //    if (!rdd.isEmpty()) {
  //      props.put("transactional.id", "my-transactional-id")
  //      val producer = new KafkaProducer(props, null, new org.apache.kafka.common.serialization.StringSerializer)
  //      producer.initTransactions()
  //      try{
  //        producer.beginTransaction()
  //        val result=rdd.collect().map(log=>{
  //          producer.send(new ProducerRecord(topic, log)).get()
  //        })
  //        producer.commitTransaction()
  //
  //        if (offset != null) {
  //          println("update offset")
  //          topics.toArray().foreach(topic => {
  //            redisRocketMQOffset.saveOffset(offset, nameServer, topic.asInstanceOf[String], group)
  //          })
  ////          saveMetrics(JunhaiLog.MONITOR_TOPIC,new Date(),Kafka2Spark,JunhaiLog.INFO,"Total send to kafka topic "+topic,"Num",result.length+"")
  //        }
  //      }catch {
  //        case ex :KafkaException=>
  //          producer.abortTransaction()
  //      }
  //    }
  //  }

  def saveErrorMetrics(document: Document, date: Date, anyRef: AnyRef, logLevel: String, logInfo: String): Unit = {
    val formatStr = "yyyy-MM-dd HH:mm:ss"
    val log_time = "log_time"
    val class_name = "class_name"
    val log_level = "log_level"
    val log_info = "info"

    val dateStr = com.ijunhai.common.TimeUtil.time2DateString(formatStr, date.getTime, com.ijunhai.common.TimeUtil.MILLISECOND)
    val doc = new Document()
    doc.append(log_time, date.getTime)
    doc.append(class_name, anyRef.getClass.getName)
    doc.append(log_level, logLevel)
    doc.append(log_info, logInfo)
    document.append(JunhaiLog.LOG, doc)
  }

  def saveMetrics(kafkaSink: Broadcast[KafkaSink], topic: String, date: Date, className: String, logLevel: String, logInfo: String, key: String = "", value: String = ""): Unit = {
    sendMetrics(kafkaSink, topic, date, className, logLevel, logInfo, key, value)
  }

  def sendMetrics(producer: KafkaProducer[Nothing, String], topic: String, date: Date, className: String, logLevel: String, logInfo: String, key: String = "", value: String = ""): Unit = {
    val doc = createMetrics(date, className, logLevel, logInfo, key, value)
    producer.send(new ProducerRecord(topic, doc.toJson))
  }

  def sendMetrics(kafkaSink: Broadcast[KafkaSink], topic: String, date: Date, className: String, logLevel: String, logInfo: String, key: String, value: String): Unit = {
    val doc = createMetrics(date, className, logLevel, logInfo, key, value)
    kafkaSink.value.syncSend(topic, doc.toJson())
  }

  def createMetrics(date: Date, className: String, logLevel: String, logInfo: String, key: String = "", value: String = ""): Document = {
    //    {
    //      "datasource":"apiClick",		//来源
    //      "fileName":"",		//文件名
    //      "host":"10.10.67.215",	//主机名
    //      "metricName":"receivedFromKafka",	//发送
    //      "tags":{},
    //      "timestamp":1516342226519,
    //      "type":"LogFile",	//
    //      "value":0
    //    }
    val formatStr = "yyyy-MM-dd HH:mm:ss"
    val log_time = "log_time"
    val class_name = "class_name"
    val log_level = "log_level"
    val log_info = "info"
    val key_ = "key"
    val value_ = "value"
    //    val dateStr=com.ijunhai.util.TimeUtil.time2DateString(formatStr,date.getTime,com.ijunhai.util.TimeUtil.MILLISECOND)
    val doc = new Document()
    doc.append(log_time, date.getTime)
    doc.append(class_name, className)
    doc.append(log_level, logLevel)
    doc.append(log_info, logInfo)
    doc.append(key_, key)
    doc.append(value_, value)
  }

  def monitor(kafkaSink: Broadcast[KafkaSink],
              topic: String,
              serverName: String,
              metricName: String,
              value: Int = 0,
              typ: String,
              fileName: String = "",
              tag: String = ""): Unit = {
    val data_source = "datasource"
    val file_name = "fileName"
    val host = "host"
    val metric_name = "metricName"
    val tags = "tags"
    val timestamp = "timestamp"
    val type_ = "type"
    val value_ = "value"
    val doc = new Document()
      .append(data_source, topic)
      .append(file_name, fileName)
      .append(host, serverName)
      .append(metric_name, metricName)
      .append(tags, tag)
      .append(timestamp, new Date().getTime + "")
      .append(type_, typ)
      .append(value_, value)

    kafkaSink.value.syncSend("metricMonitor", doc.toJson())
  }


  def initProducer(): KafkaProducer[Nothing, String] = {
    new KafkaProducer(props, null, new org.apache.kafka.common.serialization.StringSerializer)
  }

  def closeProducer(producer: KafkaProducer[Nothing, String]): Unit = {
    if (producer != null)
      producer.close()
  }
}
