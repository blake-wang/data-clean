package com.ijunhai.common.redis

import java.util
import java.util.UUID

import com.ijunhai.common.offset.redisRocketMQOffset
import org.junit.Test

/**
  * Created by Admin on 2017-08-08.
  */
@Test
class RedisClientTest {
  @Test
  def createPool(): Unit ={
    val redisConn=RedisClient.getInstatnce.getJedis
    redisConn.set("hell","what")
    println(redisConn.get("hell"))
  }
  @Test
  def redisOffsetTest(): Unit ={

//    println(redisOffset.readOffset("hello", UUID.randomUUID.toString))
  }
}
