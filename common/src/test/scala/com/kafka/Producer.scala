package com.kafka

import java.util.Properties

import com.ijunhai.common.ip.IP
import com.ijunhai.common.redis.{PropertiesUtils, RedisClient}
import com.ijunhai.storage.kafka.KafkaConstants
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.bson.Document
import org.junit.Assert._
import org.junit.Test

/**
  * Created by Admin on 2017-08-09.
  */
@Test
class Producer {
  @Test
  def producerTest() {
    val topic= PropertiesUtils.get(KafkaConstants.KAFKA_TOPIC)
    val brokers= PropertiesUtils.get(KafkaConstants.KAFKA_BROKERLIST)
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("bootstrap.servers", brokers)

    val producer = new KafkaProducer(props, null ,new org.apache.kafka.common.serialization.StringSerializer)
    producer.send(new ProducerRecord(topic, "hello world!"))
    producer.close()
  }
  @Test
  def document2json(): Unit ={
    val doc=new Document().append("a","b").append("2",2)
    println(doc.toJson)
  }
  @Test
  def redisTest(): Unit ={
    val redisConn=RedisClient.getInstatnce.getJedis
    println(redisConn.hmget("aaa","bb").get(0))
  }
  @Test
  def ipTest(): Unit ={
    val ipClass=new IP
    ipClass.load("E:\\64bit_software\\64bit_software\\17monipdb\\17monipdb.dat")
    ipClass.find("14.23.56.194").foreach(println)
  }
  @Test
  def testOK() = assertTrue(true)
}
