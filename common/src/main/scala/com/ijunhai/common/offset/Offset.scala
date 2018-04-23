
package com.ijunhai.common.offset

import java.util

import com.ijunhai.storage.redis.RedisSink
import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.JavaConversions._

/**
  * Created by Admin on 2017-08-15.
  */
object Offset {
  //  private final val redisConn=RedisClient.getInstatnce.getJedis
  def saveOffset(offsets: Array[OffsetRange], topic: String, groupId: String, redisSink: Broadcast[RedisSink]): Unit = {
    val offset_map: util.Map[String, String] = new util.HashMap[String, String]
    offsets.filter(_.topic == topic).foreach(offset => {
      offset_map.put(offset.partition + "", offset.untilOffset + "")
    })
    redisSink.value.hmset(groupId + "|" + topic, offset_map)
  }

  def readOffset(topic: String, groupId: String, redisSink: Broadcast[RedisSink]): util.HashMap[TopicPartition, Long] = {
    val map = new util.HashMap[TopicPartition, Long]()
    val init_offset_map: util.Map[String, String] = redisSink.value.hgetAll(groupId + "|" + topic)
    if (!init_offset_map.isEmpty) {
      for (brokerNameAndQueueId <- init_offset_map.keySet()) {
        map.put(new TopicPartition(topic, brokerNameAndQueueId.toInt), init_offset_map.get(brokerNameAndQueueId).toLong)
      }
    }
    map
  }

  def getOffsetGap(offsets: Array[OffsetRange], nameServer: String, topic: String, groupId: String, redisSink: Broadcast[RedisSink]): Unit = {
    val gapMap = new util.HashMap[String, Long]
    val offset_map: util.Map[String, String] = new util.HashMap[String, String]
    offsets.filter(_.topic == topic).foreach(offset => {
      offset_map.put(offset.partition + "", offset.untilOffset + "")
    })
    val init_offset_map: util.Map[String, String] = redisSink.value.hgetAll(nameServer + "|" + topic + "|" + groupId)
    if (!init_offset_map.isEmpty) {
      for (brokerNameAndQueueId <- init_offset_map.keySet()) {
        offset_map.getOrDefault(brokerNameAndQueueId, "")
      }
    }


    val result = redisSink.value.hmset(nameServer + "|" + topic + "|" + groupId, offset_map)
  }
}
