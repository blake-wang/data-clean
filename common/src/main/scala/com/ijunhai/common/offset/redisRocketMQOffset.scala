package com.ijunhai.common.offset

import java.util

import com.ijunhai.common.offset.trail.rocketMQOffset
import com.ijunhai.storage.redis.RedisSink
import org.apache.rocketmq.common.message.MessageQueue
import org.apache.rocketmq.spark.{OffsetRange, TopicQueueId}
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
/**topic和brokerName不能有|符号
  * Created by Admin on 2017-08-08.
  */
object redisRocketMQOffset extends rocketMQOffset{
//  private final val redisConn=RedisClient.getInstatnce.getJedis


  override def saveOffset(offset:java.util.Map[TopicQueueId, Array[OffsetRange]],nameServer:String, topic: String,groupId:String,redisSinkCluster: Broadcast[RedisSink]): Unit = {
    val offset_map: util.Map[String, String] = new util.HashMap[String, String]
    offset.keySet.toArray.filter(_.asInstanceOf[TopicQueueId].topic()==topic).foreach(line=>{
      val tpqid=line.asInstanceOf[TopicQueueId]
      for (or <- offset.get(tpqid)) {
        offset_map.put(or.brokerName + "|" + or.queueId, or.untilOffset + "")
        System.out.println(or)
      }
    })
    val result=redisSinkCluster.value.hmset(nameServer+"|"+topic+"|"+groupId, offset_map)
  }

  override def readOffset(nameServer:String,topic:String,groupId:String,redisSinkCluster: Broadcast[RedisSink]): util.HashMap[MessageQueue, Object] = {
    val map = new util.HashMap[MessageQueue, Object]
    val init_offset_map: util.Map[String, String] = redisSinkCluster.value.hgetAll(nameServer+"|"+topic+"|"+groupId)
    if (!init_offset_map.isEmpty) {
      for (brokerNameAndQueueId <- init_offset_map.keySet()) {
        val split: Array[String] = brokerNameAndQueueId.split('|') //0 是brokerName 1是queueID
        map.put(new MessageQueue(topic, split(0), split(1).toInt),init_offset_map.get(brokerNameAndQueueId).toLong.asInstanceOf[Object])
      }
    }
    map
  }

   def readOffsetTest(nameServer:String,topic:String,groupId:String,redisSinkCluster:RedisSink): util.HashMap[MessageQueue, Object] = {
    val map = new util.HashMap[MessageQueue, Object]
     println(nameServer+"|"+topic+"|"+groupId)
    val init_offset_map: util.Map[String, String] = redisSinkCluster.hgetAll(nameServer+"|"+topic+"|"+groupId)
    if (!init_offset_map.isEmpty) {
      for (brokerNameAndQueueId <- init_offset_map.keySet()) {
        val split: Array[String] = brokerNameAndQueueId.split('|') //0 是brokerName 1是queueID
        map.put(new MessageQueue(topic, split(0), split(1).toInt),init_offset_map.get(brokerNameAndQueueId).toLong.asInstanceOf[Object])
      }
    }
    map
  }

}
