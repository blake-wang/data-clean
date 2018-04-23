package com.ijunhai.batch

import com.ijunhai.process.agent.CoinProcess
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
//spark-submit --class com.ijunhai.batch.CoinLogBatch
// --executor-memory 3G --num-executors 5
// --conf "spark.driver.extraJavaOptions=-Xss500m"
// --conf "spark.executor.extraJavaOptions=-Xss500m"
// --conf "spark.kryoserializer.buffer.max=500m" --master yarn
// --deploy-mode cluster /data/agent_jar/data-clean-coin.jar
// hdfs://Ucluster/coinLog/wyfx/20170510/*

object CoinLogBatch {
  def main(args: Array[String]) {
    val yesterday = new DateTime().minusDays(1).toLocalDateTime.toString("yyyyMMdd")
    val sourceFilePath = "/coinLog/" + yesterday + "/*/*"
    val sparkConf = new SparkConf().setAppName("CoinLogBatch")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val kafkaSink = sc.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    CoinProcess.process(sc.textFile(sourceFilePath), "csv", kafkaSink, "coinLog")
  }
}
