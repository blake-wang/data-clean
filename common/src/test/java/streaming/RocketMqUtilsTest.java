//package streaming; /**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//import com.Deal;
//import com.ijunhai.util.XMLUtil;
//import com.ijunhai.dao.myredis.RedisSinkAlone;
//import com.ijunhai.myredis.RedisSinkCluster;
//import com.ijunhai.util.offset.redisRocketMQOffset;
//import com.ijunhai.util.redis.RedisClient;
//import com.ijunhai.common.rocketmq.RocketMQConstants;
//import com.ijunhai.util.redis.PropertiesUtils;
//import com.ijunhai.util.TimeUtil;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.rocketmq.client.exception.MQBrokerException;
//import org.apache.rocketmq.client.exception.MQClientException;
//import org.apache.rocketmq.common.message.MessageExt;
//import org.apache.rocketmq.common.message.MessageQueue;
//import org.apache.rocketmq.spark.*;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.jdom2.JDOMException;
//import org.junit.Test;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisCluster;
//
//import java.io.IOException;
//import java.io.Serializable;
//import java.io.UnsupportedEncodingException;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicReference;
//
//public class RocketMqUtilsTest implements Serializable {
//
//
//    private static String NAME_SERVER = PropertiesUtils.get(RocketMQConstants.NAMESVR);
//
//    private static String TOPIC_DEFAULT = "hello";
//
//    private static int MESSAGE_NUM = 100;
//
//    @Test
//    public void testConsumer() throws MQBrokerException, MQClientException, InterruptedException, UnsupportedEncodingException {
//
//        System.setProperty("hadoop.home.dir", PropertiesUtils.get(RocketMQConstants.HADOOP_DIR));
//        System.setProperty("spark.scheduler.mode", "FAIR");
//        Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
//        // start up spark
//        Map<String, String> optionParams = new HashMap<String, String>();
//        optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, NAME_SERVER);
//        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[*]");
//        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(3000));
//        List<String> topics = new ArrayList<String>();
//        topics.add(TOPIC_DEFAULT);
//        MessageQueue msq=new MessageQueue();
//        msq.setTopic(TOPIC_DEFAULT);
//        msq.setQueueId(1);
//        msq.setTopic("localhost.localdomain");
//        Map<MessageQueue,Object> map=new HashMap<MessageQueue, Object>();
//        map.put(msq,768035L);
//        LocationStrategy locationStrategy = LocationStrategy.PreferConsistent();
//        JavaInputDStream<MessageExt> stream = RocketMqUtils.createJavaMQPullStream(sc, UUID.randomUUID().toString(),
//                topics, ConsumerStrategy.lastest(), false, false, false, locationStrategy, optionParams);
//
//        final Set<MessageExt> result = Collections.synchronizedSet(new HashSet<MessageExt>());
//
//        stream.foreachRDD(new VoidFunction<JavaRDD<MessageExt>>() {
//            public void call(JavaRDD<MessageExt> messageExtJavaRDD) throws Exception {
//                Deal.process(messageExtJavaRDD.rdd());
//            }
//        });
//        sc.start();
//        sc.awaitTermination();
//    }
//
//    @Test
//    public void testGetOffsets() throws MQBrokerException, MQClientException, InterruptedException, UnsupportedEncodingException {
//
//        System.setProperty("hadoop.home.dir", PropertiesUtils.get(RocketMQConstants.HADOOP_DIR));
//        System.setProperty("spark.scheduler.mode", "FAIR");
//        Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
////        final String uuid=UUID.randomUUID().toString();
//        final String uuid="group";
//        Map<String, String> optionParams = new HashMap<String, String>();
//        optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, NAME_SERVER);
//        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[*]");
//        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(1000));
//        List<String> topics = new ArrayList<String>();
//        topics.add(TOPIC_DEFAULT);
//        Map<MessageQueue,Object> map=new HashMap<MessageQueue, Object>();
//        Broadcast<RedisSinkAlone> redisSinkCluster = sc.sparkContext().broadcast(RedisSinkAlone.apply());
//
//        LocationStrategy locationStrategy = LocationStrategy.PreferConsistent();
//        JedisCluster redisConn= RedisClient.getInstatnce().getJedis();
//        assert redisConn != null;
////        String test=redisConn.get("a");
////        System.out.println(test);
////        Map<String,String> init_offset_map=redisConn.hgetAll(UUID.randomUUID().toString()+"|hello");
////        if (init_offset_map.isEmpty()){
////            map.put(new MessageQueue("hello","localhost.localdomain",6),768977L);
////            map.put(new MessageQueue("hello","localhost.localdomain",7),769011L);
////            map.put(new MessageQueue("hello","localhost.localdomain",4),769046L);
////            map.put(new MessageQueue("hello","localhost.localdomain",5),769006L);
////            map.put(new MessageQueue("hello","localhost.localdomain",2),769040L);
////            map.put(new MessageQueue("hello","localhost.localdomain",3),769019L);
////            map.put(new MessageQueue("hello","localhost.localdomain",0),768998L);
////            map.put(new MessageQueue("hello","localhost.localdomain",1),769040L);
////        }else {
////            for (String brokerNameAndQueueId:init_offset_map.keySet()){
////                String[] split = brokerNameAndQueueId.split("|");
////                map.put(new MessageQueue(TOPIC_DEFAULT,split[0],Integer.parseInt(split[1])),Long.parseLong(init_offset_map.get(brokerNameAndQueueId)));
////            }
////        }
//
//        JavaInputDStream<MessageExt> dStream = RocketMqUtils.createJavaMQPullStream(sc, uuid,
//                topics, ConsumerStrategy.specificOffset(redisRocketMQOffset.readOffset(NAME_SERVER,TOPIC_DEFAULT,uuid,redisSinkCluster)), false, true, true, locationStrategy, optionParams);
//
//        // hold a reference to the current offset ranges, so it can be used downstream
//        final AtomicReference<Map<TopicQueueId, OffsetRange[]>> offsetRanges = new AtomicReference<Map<TopicQueueId, OffsetRange[]>>();
//        final Set<MessageExt> result = Collections.synchronizedSet(new HashSet<MessageExt>());
//
//        dStream.transform(new Function<JavaRDD<MessageExt>, JavaRDD<MessageExt>>() {
//            public JavaRDD<MessageExt> call(JavaRDD<MessageExt> v1) throws Exception {
//                Map<TopicQueueId, OffsetRange []> offsets = ((HasOffsetRanges) v1.rdd()).offsetRanges();
//                offsetRanges.set(offsets);
//                return v1;
//            }
//        }).foreachRDD(new VoidFunction<JavaRDD<MessageExt>>() {
//            public void call(JavaRDD<MessageExt> messageExtJavaRDD) throws Exception {
//                result.addAll(messageExtJavaRDD.collect());
//            }
//        });
//
//        sc.start();
//
//        long startTime = System.currentTimeMillis();
//        boolean matches = false;
//        while (!matches && System.currentTimeMillis() - startTime < 20000) {
//            matches = MESSAGE_NUM == result.size();
//            Thread.sleep(50);
//        }
//
//        Map<TopicQueueId, OffsetRange []> offset=offsetRanges.get();
//        redisRocketMQOffset.saveOffset( offset,NAME_SERVER,TOPIC_DEFAULT,uuid,redisSinkCluster);
////        for (TopicQueueId  tpqid:offset.keySet()){
////            System.out.print(tpqid+"=");
////            Map<String,String> offset_map=new HashMap<String, String>();
////            for (OffsetRange or:offset.get(tpqid)){
////                offset_map.put(or.brokerName()+"|"+or.queueId(),or.untilOffset()+"");
////                System.out.print(or+",");
////            }
////            redisConn.hmset(tpqid.topic(),offset_map);
////            System.out.println();
////        }
//        for (MessageExt me:result){
//            System.out.println(me);
//        }
//        sc.stop();
//    }
//    @Test
//    public void XMLUtilTest(){
//        try {
//            org.bson.Document doc=XMLUtil.xml2bson("D:/testFile/cleanConfig.xml");
//            System.out.println(doc.get("order"));
//        } catch (JDOMException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//    @Test
//    public void timeUtilTest(){
//        System.out.println(TimeUtil.time2DateString("yyyy-MM-dd",1504146813000L,TimeUtil.MILLISECOND));
//    }
//}
