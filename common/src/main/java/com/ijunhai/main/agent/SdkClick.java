package com.ijunhai.main.agent;

import com.ijunhai.common.offset.redisRocketMQOffset;
import com.ijunhai.process.agent.AgentProcess;
import com.ijunhai.storage.kafka.KafkaSink;
import com.ijunhai.storage.kafka.Save2Kafka;
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
//dalanClick:
//SdkClick

public class SdkClick implements Serializable {
    private static String GROUP = "SdkClick";

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("spark.scheduler.mode", "FAIR");
        Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

        final String nameServer = args[0];
        String topic = args[1]; //coinLog
        String second = args[2]; //60
//        final String nameServer = "bjc-tjlog.ijunhai.net:19876";
//        String topic = "coinLog";
//        String second = "60";


        if (nameServer == null || nameServer.equals("") || topic == null || topic.equals("") || second == null || second.equals("")) {
            System.err.println("<SERVER:PORT> <SRCTOPIC> <SECOND> <COLLECT_IPS> [DESTOPIC]");
            System.exit(1);
        }

        Map<String, String> optionParams = new HashMap<String, String>();
        optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, nameServer);
//        optionParams.put(RocketMQConfig.PULL_TIMEOUT_MS,"60000");
//        optionParams.put(RocketMQConfig.CONSUMER_GROUP,"testOrder");
        String uuid = UUID.randomUUID().toString();
        String appName = nameServer + ":SdkClick";
        SparkConf sparkConf = new SparkConf().setAppName(appName);
//                .setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(Integer.parseInt(second) * 1000));
        Broadcast kafkaSink = sc.sparkContext().broadcast(KafkaSink.apply(Save2Kafka.brokers()));

        final List<String> topics = new ArrayList<String>();
        Collections.addAll(topics, topic.split(","));
        LocationStrategy locationStrategy = LocationStrategy.PreferConsistent();
        Broadcast redisSinkCluster = sc.sparkContext().broadcast(RedisSink.apply());

        Map<MessageQueue, Object> queueToOffset = new HashMap<MessageQueue, Object>();//获得所有需要消费的topic的broker信息
        for (String t : topics) {
            queueToOffset.putAll(redisRocketMQOffset.readOffset(nameServer, t, GROUP, redisSinkCluster));
        }

        JavaInputDStream<MessageExt> dStream;
        if (!queueToOffset.isEmpty()) {
            dStream = RocketMqUtils.createJavaMQPullStream(sc, GROUP, topics,
                    ConsumerStrategy.specificOffset(queueToOffset), false, true, true, locationStrategy, optionParams);
        } else {
            dStream = RocketMqUtils.createJavaMQPullStream(sc, GROUP, topics,
                    ConsumerStrategy.earliest(), false, false, true, locationStrategy, optionParams);
        }

//        dStream = RocketMqUtils.createJavaMQPullStream(sc, GROUP, topics,
//                ConsumerStrategy.lastest(), false, false, true, locationStrategy, optionParams);
//

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

                if (topic.contains("dalan")) {
                    AgentProcess.dalanSDK(messageExtJavaRDD.rdd(), kafkaSink, topic);
                } else {
                    AgentProcess.sdkProcess(messageExtJavaRDD.rdd(), kafkaSink, topic);
                }

                if (offsetRanges.get() != null) {
                    System.out.println("update offset");
                    topics.forEach(topic -> {
                        redisRocketMQOffset.saveOffset(offsetRanges.get(), nameServer, topic, GROUP, redisSinkCluster);
                    });
                }

            }
        });
        sc.start();
        sc.awaitTermination();

    }
}
