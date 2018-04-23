package com.ijunhai.storage.redis

import com.ijunhai.common.redis.PropertiesUtils
import com.ijunhai.common.redis.RedisClientConstants._
import org.apache.commons.pool2.impl.GenericObjectPoolConfig._
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class RedisSinkAlone(getConnect: () => Jedis) extends Serializable {

	lazy val jedis = getConnect()

	def set(key: String, value: String): String = {
		jedis.set(key,value)
	}

	def setex(key: String, second: Int, value: String): String ={
		jedis.setex(key,second,value)
	}
}

object RedisSinkAlone {
	def apply(): RedisSinkAlone = {
		val f = () => {
			val config: JedisPoolConfig = new JedisPoolConfig
			val host: String = "redis-client"
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
			val jedis=pool.getResource
			sys.addShutdownHook {
				jedis.close()
			}
			jedis
		}
		new RedisSinkAlone(f)
	}
}
