package com.ijunhai.batch.agent

import com.ijunhai.batch.process.LogBatchProcess
import com.ijunhai.common.HDFSUtil
import com.ijunhai.process.jhchannel.DB2GPProcess
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-submit --class com.ijunhai.batch.OldLoginLogBatch --executor-memory 5G --num-executors 10 --conf "spark.driver.extraJavaOptions=-Xss500m"  --conf "spark.executor.extraJavaOptions=-Xss500m" --conf "spark.kryoserializer.buffer.max=500m" --master yarn --deploy-mode cluster /data/data-clean.jar hdfs://Ucluster/data/agent/agent_login_old/agent_login_log_201711.txt agent hdfs://Ucluster/data/config/old-agent-cleanConfig.xml hdfs://Ucluster/data/ip_database/17monipdb.dat hdfs://Ucluster/data/agent/afterclean_11111
  *
  * spark-submit --class com.ijunhai.OldLoginLogBatch --executor-memory 5G --num-executors 10
  * --conf "spark.driver.extraJavaOptions=-Xss500m"  --conf "spark.executor.extraJavaOptions=-Xss500m"
  * --conf "spark.kryoserializer.buffer.max=500m" --master yarn
  * --deploy-mode cluster /work/bigdata/data-clean.jar
  * hdfs://Ucluster/data/login_all/agent_login_old/agent_login_log_201601.txt
  * agent
  * hdfs://Ucluster/data/config/old-agent-cleanConfig.xml
  * hdfs://Ucluster/data/ip_database/17monipdb.dat
  * hdfs://Ucluster/data/agent/afterclean/201601JunhaiLog.scala
  */
object CmDBBatch {
  def main(args: Array[String]) {
    val sourceFilePath = args(0) //  hdfs://Ucluster/data/cm_all/order_201802.txt
    val system = args(1)    //chumeng
    val dbType = args(2)    //user 或者 order

    val sparkConf = new SparkConf()
      //      .setMaster("local")
      .setAppName("CmDBBatch")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val redisSinkCluster = sc.broadcast(RedisSink.apply)
    val gpSink = sc.broadcast(GreenPlumSink.apply(DB2GPProcess.CHUMENG_DATABASE))
    val rdd: RDD[String] = sc.textFile(sourceFilePath)
    LogBatchProcess.cmDBProcess(rdd, system, dbType, gpSink, redisSinkCluster)
  }
}
