/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.spark

import org.apache.rocketmq.client.consumer.{DefaultMQPullConsumer, PullResult, PullStatus}
import org.apache.rocketmq.common.message.{MessageExt, MessageQueue}
import java.{util => ju}

import org.apache.rocketmq.remoting.exception.RemotingTimeoutException

/**
  * Consumer of single topic partition, intended for cached reuse.
  */

private[rocketmq]
class CachedMQConsumer private(
   val groupId: String,
   val client: DefaultMQPullConsumer,
   val topic: String,
   val queueId: Int,
   val names: Set[String],
   val optionParams: ju.Map[String, String]) extends Logging {

  private val maxBatchSize =
    if(optionParams.get(RocketMQConfig.PULL_MAX_BATCH_SIZE)!=null){
      optionParams.get(RocketMQConfig.PULL_MAX_BATCH_SIZE).toInt
    }else{
      32
    }

  private var buffer = names.map(name => name -> ju.Collections.emptyList[MessageExt].iterator).toMap

  private var nextOffsets = names.map(name => name -> -2L).toMap


  /**
    * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
    * Sequential forward access will use buffers, but random access will be horribly inefficient.
    */
  def get(name: String, queueOffset: Long): MessageExt = {

    val nextOffset = nextOffsets(name)
    logDebug(s"Get $groupId $topic $queueId brokerName $name nextOffset $nextOffset requested")

    if (queueOffset != nextOffset) {
      logInfo(s"Initial fetch for $groupId $topic $name $queueOffset")
      poll(name, queueOffset)
    }

    if (!buffer(name).hasNext) {
      poll(name, queueOffset)
    }

    val iter = buffer(name)
    if(iter.hasNext) {
      val record = iter.next
      assert(record.getQueueOffset == queueOffset,
        s"Got wrong record for $groupId $topic $queueId $name even after seeking to offset $queueOffset")
      nextOffsets += (name -> (queueOffset + 1))
      record
    } else {
//      logError(s"Failed to get records for $groupId $topic $queueId $name $queueOffset after polling ")
      println(s"Failed to get records for $groupId $topic $queueId $name $queueOffset after polling ")
      new MessageExt()
    }
  }

  private def poll(name: String, queueOffset: Long) {
    val p = try{
       client.pull(new MessageQueue(topic, name, queueId), "*", queueOffset, maxBatchSize)
    }catch {
      case e:RemotingTimeoutException=>
//        logError(s"Failed to get records for $groupId $topic $queueId $name $queueOffset after polling cause by RemotingTimeoutException")
        new PullResult(PullStatus.NO_NEW_MSG,0,0,0,null)
    }
    if (p.getPullStatus == PullStatus.FOUND){
      buffer += (name -> p.getMsgFoundList.iterator)
    } else {
//      logError(s"Failed to get records for $groupId $topic $queueId $name $queueOffset after polling ")
      Thread.sleep(100)
    }
  }
}

object CachedMQConsumer extends Logging {
  
  private case class CacheKey(groupId: String, topic: String, queueId: Int, names: Set[String])

  private var groupIdToClient = Map[String, DefaultMQPullConsumer]()

  // Don't want to depend on guava, don't want a cleanup thread, use a simple LinkedHashMap
  private var cache: ju.LinkedHashMap[CacheKey, CachedMQConsumer] = null

  /** Must be called before get, once per JVM, to configure the cache. Further calls are ignored */
  def init(
    initialCapacity: Int,
    maxCapacity: Int,
    loadFactor: Float): Unit = CachedMQConsumer.synchronized {
    if (null == cache) {
      logInfo(s"Initializing cache $initialCapacity $maxCapacity $loadFactor")
      cache = new ju.LinkedHashMap[CacheKey, CachedMQConsumer](
        initialCapacity, loadFactor, true) {
        override def removeEldestEntry(
            entry: ju.Map.Entry[CacheKey, CachedMQConsumer]): Boolean = {
          if (this.size > maxCapacity) {
            true
          } else {
            false
          }
        }
      }
    }
  }

  /**
    * Get a cached consumer for groupId, assigned to topic, queueId and names.
    * If matching consumer doesn't already exist, will be created using optionParams.
    */
  def getOrCreate(
           groupId: String,
           topic: String,
           queueId: Int,
           names: Set[String],
           optionParams: ju.Map[String, String]): CachedMQConsumer =
  CachedMQConsumer.synchronized {

    val client = if (!groupIdToClient.contains(groupId)){
        val client = RocketMqUtils.mkPullConsumerInstance(groupId, optionParams, s"$groupId-executor")
      groupIdToClient += groupId -> client
      client
    } else {
      groupIdToClient(groupId)
    }

    val k = CacheKey(groupId, topic, queueId, names)
    if (cache.containsValue(k)) {
      cache.get(k)
    } else {
      logInfo(s"Cache miss for $k")
      logDebug(cache.keySet.toString)
      val  c= new CachedMQConsumer(groupId, client, topic, queueId, names, optionParams)
      cache.put(k, c)
      c
    }
  }

  /**
    * Get a fresh new instance, unassociated with the global cache.
    * Caller is responsible for closing
    */
  def getUncached(
                   groupId: String,
                   topic: String,
                   queueId: Int,
                   names: Set[String],
                   optionParams: ju.Map[String, String]): CachedMQConsumer = {
    val client = RocketMqUtils.mkPullConsumerInstance(groupId, optionParams, 
      s"$groupId-executor-$queueId-${names.mkString("-")}")
    new CachedMQConsumer(groupId, client, topic, queueId, names, optionParams)
  }

  /** remove consumer for given groupId, topic, and queueId, if it exists */
  def remove(groupId: String, topic: String, queueId: Int, names: Set[String]): Unit = {
    val k = CacheKey(groupId, topic, queueId, names)
    logInfo(s"Removing $k from cache")
    val v = CachedMQConsumer.synchronized {
      cache.remove(k)
    }
  }
}
