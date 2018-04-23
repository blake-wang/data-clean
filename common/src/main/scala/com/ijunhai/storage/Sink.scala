package com.ijunhai.storage

import com.ijunhai.storage.redis.RedisSink
import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by Admin on 2017-08-15.
  */
trait Sink {
  def apply()
}
