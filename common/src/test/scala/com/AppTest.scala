package com

import java.util.Properties

import com.ijunhai.common.XMLUtil
import com.ijunhai.common.redis.PropertiesUtils
import com.ijunhai.common.rocketmq.RocketMQConstants
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy, RocketMQConfig, RocketMqUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document
import org.junit.Assert._
import org.junit._

import scala.collection.{JavaConverters, mutable}


@Test
class AppTest {
@Test
  def testConsumer(): Unit ={
    System.setProperty("hadoop.home.dir", PropertiesUtils.get(RocketMQConstants.HADOOP_DIR) )
    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
    val optionParam= mutable.HashMap[String,String]()
    optionParam.put(RocketMQConfig.NAME_SERVER_ADDR,"localhost:9876")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    val topic=List(PropertiesUtils.get(RocketMQConstants.ROCKETMQ_TOPIC))

    val stream=RocketMqUtils.createMQPullStream(ssc,"TagA",JavaConverters.seqAsJavaListConverter(topic).asJava,ConsumerStrategy.earliest,
      true,false,false,LocationStrategy.PreferConsistent,JavaConverters.mapAsJavaMapConverter(optionParam).asJava)
    stream.foreachRDD(rdd=>{
      rdd.foreach(println)
    })
    ssc.start()
    ssc.awaitTermination()
  }
  @Test
  def producerTest(args: Array[String]) {
    val topic= PropertiesUtils.get(RocketMQConstants.ROCKETMQ_TOPIC)
    val brokers= PropertiesUtils.get(RocketMQConstants.NAMESVR)
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", brokers)

    val producer = new KafkaProducer(props, null ,new org.apache.kafka.common.serialization.StringSerializer)
    producer.send(new ProducerRecord(topic, "hello world!"))

    producer.close()

  }
  @Test
  def XMLUtilTest(){
    val doc=XMLUtil.xml2bson("d:/testFile/cleanConfig.xml")
    val value=doc.get("order").asInstanceOf[Document].get("order").asInstanceOf[Document].getString("order_sn")
    println(value)

  }
  @Test
  def documentTest(): Unit ={

  }
  @Test
  def testOK() = assertTrue(true)

//    @Test
//    def testKO() = assertTrue(false)
}


