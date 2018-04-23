package com.ijunhai.main.agent;

import com.ijunhai.common.logsystem.Monitor;
import com.ijunhai.common.offset.redisRocketMQOffset;
import com.ijunhai.process.agent.AgentProcess;
import com.ijunhai.storage.greenplum.GreenPlumSink;
import com.ijunhai.storage.kafka.KafkaSink;
import com.ijunhai.storage.redis.RedisSink;
import com.ijunhai.storage.redis.Save2Redis;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.spark.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


public class Mq2Kafka implements Serializable {
    private static String GROUP = "Mq2Kafka";
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("spark.scheduler.mode", "FAIR");
        Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        if (args.length != 4) {
            System.err.println("<MQSVR:PORT> <TOPIC1,TOPIC2,...> <SECOND> <KFKSVR:PORT>");
            System.exit(1);
        }
        final String nameServer = args[0];
        String topicsStr = args[1];
        String second = args[2];
        String brokers = args[3];
        if (nameServer == null || nameServer.equals("") || topicsStr == null || topicsStr.equals("") || second == null || second.equals("") || brokers == null || brokers.equals("")) {
            System.err.println("<MQSVR:PORT> <TOPIC1,TOPIC2,...> <SECOND> <KFKSVR:PORT>");
            System.exit(1);
        }
        AgentProcess.setBrokers(brokers);
        Map<String, String> optionParams = new HashMap<String, String>();
        optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, nameServer);
        optionParams.put(RocketMQConfig.PULL_TIMEOUT_MS, "60000");
        String uuid = UUID.randomUUID().toString();
        final List<String> topics = new ArrayList<String>();
        Collections.addAll(topics, topicsStr.split(","));
        String appName;
        if (topicsStr.contains("agentLogin")) {
            appName = nameServer + ":Mq2KafkaOld";
        } else {
            appName = nameServer + ":Mq2Kafka";
        }

        SparkConf sparkConf = new SparkConf().setAppName(appName);
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(Integer.parseInt(second) * 1000));
        Broadcast kafkaSinkBroadcast = sc.sparkContext().broadcast(KafkaSink.apply(brokers));
        Broadcast GPSink = sc.sparkContext().broadcast(GreenPlumSink.apply(Monitor.database()));
        LocationStrategy locationStrategy = LocationStrategy.PreferConsistent();
        Broadcast redisSinkCluster = sc.sparkContext().broadcast(RedisSink.apply());
        Map<MessageQueue, Object> queueToOffset = new HashMap<MessageQueue, Object>();
        for (String topic : topics) {
            queueToOffset.putAll(redisRocketMQOffset.readOffset(nameServer, topic, GROUP, redisSinkCluster));
        }

        JavaInputDStream<MessageExt> dStream;
        if (!queueToOffset.isEmpty()) {
            dStream = RocketMqUtils.createJavaMQPullStream(sc, GROUP, topics,
                    ConsumerStrategy.specificOffset(queueToOffset), false, true, true, locationStrategy, optionParams);
        } else {
            dStream = RocketMqUtils.createJavaMQPullStream(sc, GROUP, topics,
                    ConsumerStrategy.lastest(), false, false, true, locationStrategy, optionParams); //线上环境需要改成earlist
        }

        final AtomicReference<Map<TopicQueueId, OffsetRange[]>> offsetRanges = new AtomicReference<Map<TopicQueueId, OffsetRange[]>>();

        dStream.transform(new Function<JavaRDD<MessageExt>, JavaRDD<MessageExt>>() {
            public JavaRDD<MessageExt> call(JavaRDD<MessageExt> v1) throws Exception {
                Map<TopicQueueId, OffsetRange[]> offsets = ((HasOffsetRanges) v1.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return v1;
            }
        }).foreachRDD(new VoidFunction<JavaRDD<MessageExt>>() {
            public void call(JavaRDD<MessageExt> messageExtJavaRDD) throws Exception {
                if (Save2Redis.isRunning(appName, uuid, Integer.parseInt(second), redisSinkCluster)) {
                    System.out.println(appName + " is running and the uuid is " + uuid);
                    System.exit(1);
                }
//                System.out.println(messageExtJavaRDD.rdd().count());
                AgentProcess.logProcess(messageExtJavaRDD.rdd(), kafkaSinkBroadcast, GPSink, offsetRanges.get(), nameServer, topics, GROUP, redisSinkCluster);
            }
        });

        sc.start();
        sc.awaitTermination();
    }
}
