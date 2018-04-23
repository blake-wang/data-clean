package com.ijunhai.storage.kafka

import java.util.Properties

import com.ijunhai.common.redis.PropertiesUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by Admin on 2017-08-18.
  */
object Producer {
  val TOPIC = PropertiesUtils.get(KafkaConstants.KAFKA_TOPIC)
  val CONTENT="NORMAL: [2017-09-14 14:28:42] CIP[175.42.89.164] DATA[{\"user\":{\"user_id\":\"2004734\",\"user_name\":\"\",\"gender\":\"\",\"birth\":\"\",\"age\":\"\"},\"game\":{\"game_id\":\"121\",\"game_name\":\"\",\"game_ver\":\"\"},\"agent\":{\"channel_id\":\"10121\",\"game_channel_id\":\"101843\",\"access_token\":\"MTUwNTM3MDUyMCMwMWI5ZGIyMWMxOWY0OTNiMjY4Mjc2ZWRjMzkxOWVmZg==\"},\"device\":{\"screen_height\":\"897\",\"screen_width\":\"540\",\"device_id\":\"ffffffff-9eb1-0424-ffff-ffff8592f4ef\",\"ios_idfa\":\"\",\"android_imei\":\"862089030679047\",\"android_adv_id\":\"\",\"android_id\":\"\",\"device_name\":\"Weiimi X3\",\"os_ver\":\"6.0\",\"sdk_ver\":\"2.1\",\"package_name\":\"com.sq.ztxj.hd\",\"os_type\":\"android\",\"net_type\":\"\",\"user_agent\":\"\"},\"event\":\"login\",\"is_test\":\"regular\",\"data_ver\":\"1.0\",\"client_time_zone\":\"\",\"server_time_zone\":\"+08:00\",\"client_ts\":0,\"server_ts\":1505370522,\"client_ip\":\"175.42.89.164\"}]"
  val BROKER_LIST = PropertiesUtils.get(KafkaConstants.KAFKA_BROKERLIST)
  val SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类
  def main(args: Array[String]) {
    val props = new Properties()
    props.put("bootstrap.servers", args(0))
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    for(i <- 0 until 10){
      producer.send(new ProducerRecord[String, String](args(1), Integer.toString(i), CONTENT))
      //      producer.send(new ProducerRecord[String, String]("channel", Integer.toString(i), CONTENT))
    }
    producer.close()
  }
}
