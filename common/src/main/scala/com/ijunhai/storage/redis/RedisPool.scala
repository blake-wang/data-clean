package com.ijunhai.storage.redis

import com.ijunhai.common.redis.PropertiesUtils
import com.ijunhai.common.redis.RedisClientConstants._
import org.apache.commons.pool2.impl.BaseObjectPoolConfig._
import org.apache.commons.pool2.impl.GenericObjectPoolConfig._
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

/**
  * Created by Admin on 2017-09-11.
  */
object RedisPool {
  var instances = Map[String, JedisCluster]()
  //node1:port1,node2:port2 -> node
  def nodes2ServerList(nodes: String): java.util.Set[HostAndPort] = {
    val serverList = new java.util.HashSet[HostAndPort]()
    nodes.split(",")
      .map(portNode => portNode.split(":"))
      .flatMap { ar => {
        if (ar.length == 2) {
          Some(ar(0), ar(1).toInt)
        } else {
          None
        }
      }
      }
      .foreach { case (node, port) => serverList.add(new HostAndPort(node, port)) }
    serverList
  }

  def apply(nodes: String): JedisCluster = {
    instances.getOrElse(nodes, {
      val servers = nodes2ServerList(nodes)
      val config: JedisPoolConfig = new JedisPoolConfig
      // set jedis instance amount, default 8
      config.setMaxTotal(PropertiesUtils.getInt(REDIS_MaxTotal, DEFAULT_MAX_TOTAL))
      // set max idle jedis instance amount, default 8
      config.setMaxIdle(PropertiesUtils.getInt(REDIS_MaxIdle, DEFAULT_MAX_IDLE))
      // set min idle jedis instance amount, default 0
      config.setMinIdle(PropertiesUtils.getInt(REDIS_MinIdle, DEFAULT_MIN_IDLE))
      // max wait time when borrow a jedis instance
      config.setMaxWaitMillis(PropertiesUtils.getLong(REDIS_MaxWaitMillis, DEFAULT_MAX_WAIT_MILLIS))
      lazy val client = new JedisCluster(servers, config)
      instances += nodes -> client
      println("new client added")
      client
    })
  }
}
