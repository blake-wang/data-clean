package com.ijunhai.storage.redis

import java.{lang, util}

import com.ijunhai.common.redis.PropertiesUtils
import com.ijunhai.common.redis.RedisClientConstants._
import org.apache.commons.pool2.impl.GenericObjectPoolConfig._
import redis.clients.jedis._

/**
  * Created by Admin on 2017-11-23.
  */
class RedisSink(getConnect: () => Jedis) extends Serializable {
  lazy val jedis = getConnect()

  def set(key: String, value: String): String = {
    jedis.set(key, value)
  }

  def setex(key: String, second: Int, value: String): String = {
    jedis.setex(key, second, value)
  }

  def hmset(key: String, value: util.Map[String, String]): Any = {
    jedis.hmset(key, value)
  }

  def hgetAll(key: String): util.Map[String, String] = {
    jedis.hgetAll(key)
  }

  def get(key: String): String = {
    jedis.get(key)
  }

  def hmget(key: String, second: String): util.List[String] = {
    jedis.hmget(key, second)
  }

  def hmget(key: String, second: String, third: String): util.List[String] = {
    jedis.hmget(key, second, third)
  }

  def hmget(key: String, second: String, third: String, forth: String): util.List[String] = {
    jedis.hmget(key, second, third, forth)
  }

  def hmget(key: String, second: String, third: String, forth: String, five: String): util.List[String] = {
    jedis.hmget(key, second, third, forth, five)
  }

  def hset(key: String, second: String, third: String): lang.Long = {
    jedis.hset(key, second, third)
  }

  def exists(key: String): Boolean = {
    jedis.exists(key)
  }

  def close(): Unit = {
    if (jedis != null) {
      jedis.close()
    }
  }
}

object RedisSink {
  def apply(): RedisSink = {
    val f = () => {
      val config: JedisPoolConfig = new JedisPoolConfig
      val host: String = "10.13.134.171"
      //        val host: String = "192.168.1.110"
      //        val port: Int = 6381

      val port: Int = 6379
      // set jedis instance amount, default 8
      config.setMaxTotal(PropertiesUtils.getInt(REDIS_MaxTotal, 1000))
      // set max idle jedis instance amount, default 8
      config.setMaxIdle(PropertiesUtils.getInt(REDIS_MaxIdle, 1000))
      // set min idle jedis instance amount, default 0
      config.setMinIdle(PropertiesUtils.getInt(REDIS_MinIdle, DEFAULT_MIN_IDLE))
      // max wait time when borrow a jedis instance
      config.setMaxWaitMillis(PropertiesUtils.getLong(REDIS_MaxWaitMillis, 60000))
      config.setTestOnBorrow(true)
      val REDIS_TIMEOUT: Int = 3000
      val pool = new JedisPool(config, host, port, REDIS_TIMEOUT)
      val jedis = pool.getResource
      sys.addShutdownHook {
        jedis.close()
      }
      jedis
    }
    new RedisSink(f)
  }

  def apply(host: String, port: Int): RedisSink = {
    val f = () => {
      val config: JedisPoolConfig = new JedisPoolConfig



      // set jedis instance amount, default 8
      config.setMaxTotal(PropertiesUtils.getInt(REDIS_MaxTotal, 1000))
      // set max idle jedis instance amount, default 8
      config.setMaxIdle(PropertiesUtils.getInt(REDIS_MaxIdle, 1000))
      // set min idle jedis instance amount, default 0
      config.setMinIdle(PropertiesUtils.getInt(REDIS_MinIdle, DEFAULT_MIN_IDLE))
      // max wait time when borrow a jedis instance
      config.setMaxWaitMillis(PropertiesUtils.getLong(REDIS_MaxWaitMillis, 60000))
      config.setTestOnBorrow(true)
      val REDIS_TIMEOUT: Int = 3000
      val pool = new JedisPool(config, host, port, REDIS_TIMEOUT)
      val jedis = pool.getResource
      sys.addShutdownHook {
        jedis.close()
      }
      jedis
    }
    new RedisSink(f)
  }
}
