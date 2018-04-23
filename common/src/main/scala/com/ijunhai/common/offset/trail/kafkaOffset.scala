package com.ijunhai.common.offset.trail

import com.ijunhai.storage.redis.{RedisSinkAlone, RedisSink}
import org.apache.kafka.common.TopicPartition
import org.apache.rocketmq.spark.TopicQueueId
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by Admin on 2017-08-15.
  */
trait kafkaOffset {
  def saveOffset(offsets:Array[OffsetRange],nameServer:String,topic: String,groupId:String,redisSink: Broadcast[RedisSink]): Unit
  def readOffset(nameServer:String,topic:String,groupId:String,redisSink: Broadcast[RedisSink]): java.util.HashMap[TopicPartition,Long]
}
