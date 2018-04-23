package com.ijunhai.common.offset.trail

import com.ijunhai.storage.redis.RedisSink
import org.apache.rocketmq.common.message.MessageQueue
import org.apache.rocketmq.spark.{OffsetRange, TopicQueueId}
import org.apache.spark.broadcast.Broadcast

/**
  * Created by Admin on 2017-08-08.
  */
trait rocketMQOffset {
  def saveOffset(offset:java.util.Map[TopicQueueId, Array[OffsetRange]],nameServer:String,topic: String,groupId:String,redisSink: Broadcast[RedisSink]): Unit
  def readOffset(nameServer:String,topic:String,groupId:String,redisSink: Broadcast[RedisSink]): java.util.HashMap[MessageQueue,Object]
}
