package com.ijunhai.main;

import com.ijunhai.common.logsystem.Monitor;
import com.ijunhai.common.offset.redisRocketMQOffset;
import com.ijunhai.process.CommonProcess;
import com.ijunhai.storage.greenplum.GreenPlumSink;
import com.ijunhai.storage.hbase.HbaseSink;
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
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.LocationStrategy;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


public class Dalan implements Serializable {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("spark.scheduler.mode", "FAIR");
        Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);


        String source = args[0];   // dalanLoginSrc/dalanDBSrc
        String topicsStr = args[1]; //dalan  /  dlUserOrder
        String nameServer = "bjc-apilog.dalan.net:19876";
        String group = "agent";
        Map<String, String> optionParams = new HashMap<String, String>();
        optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, nameServer);

        String uuid = UUID.randomUUID().toString();
        String appName = "agent:" + source;

        SparkConf sparkConf = new SparkConf().setAppName(appName);
//                .setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(Integer.parseInt("60") * 1000));
        final List<String> topics = new ArrayList<String>();
        Collections.addAll(topics, topicsStr.split(","));
        LocationStrategy locationStrategy = LocationStrategy.PreferConsistent();

        Broadcast redisSink = sc.sparkContext().broadcast(RedisSink.apply());
        Broadcast gpSink = sc.sparkContext().broadcast(GreenPlumSink.apply(Monitor.database()));
        Broadcast kafkaSink = sc.sparkContext().broadcast(KafkaSink.apply(Save2Kafka.brokers()));
        Broadcast hbaseSink = sc.sparkContext().broadcast(HbaseSink.apply());

        Map<MessageQueue, Object> queueToOffset = new HashMap<MessageQueue, Object>();
        for (String topic : topics) {
            queueToOffset.putAll(redisRocketMQOffset.readOffset(nameServer, topic, group, redisSink));
        }
        JavaInputDStream<MessageExt> dStream;
        if (!queueToOffset.isEmpty()) {
            dStream = RocketMqUtils.createJavaMQPullStream(sc, group, topics,
                    ConsumerStrategy.specificOffset(queueToOffset), false, true, true, locationStrategy, optionParams);
        } else {
            dStream = RocketMqUtils.createJavaMQPullStream(sc, group, topics,
                    ConsumerStrategy.earliest(), false, false, true, locationStrategy, optionParams);
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
                if (Save2Redis.isRunning(appName, uuid, Integer.parseInt("60"), redisSink)) {
                    System.out.println(appName + " is running and the uuid is " + uuid);
                    System.exit(1);
                }

                if (source.contains("Login")) {
                    CommonProcess.loginProcess(CommonProcess.extraOnly(messageExtJavaRDD.rdd()), source, redisSink, kafkaSink, gpSink, hbaseSink);
                } else {
                    CommonProcess.orderProcess(CommonProcess.extraOnly(messageExtJavaRDD.rdd()), source, redisSink, kafkaSink, gpSink, hbaseSink);
                }

                if (offsetRanges.get() != null) {
                    topics.forEach(topic -> {
                        redisRocketMQOffset.saveOffset(offsetRanges.get(), nameServer, topic, group, redisSink);
                    });
                }
            }
        });
        sc.start();
        sc.awaitTermination();
    }
}
