package com.ijunhai.main.jhchannel;

import com.ijunhai.common.logsystem.Monitor;
import com.ijunhai.process.CommonProcess;
import com.ijunhai.process.jhchannel.ClickProcess;
import com.ijunhai.storage.greenplum.GreenPlumSink;
import com.ijunhai.storage.kafka.KafkaSink;
import com.ijunhai.storage.kafka.Save2Kafka;
import com.ijunhai.storage.redis.RedisSinkAlone;
import com.ijunhai.storage.redis.RedisSink;
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


public class AdvClickTest implements Serializable {
    private static String GROUP = "AdvClick";
    //海外  alhk.hw.net:19876             hwUserOrder haiwai  collect_metric
//大蓝  bjc-apilog.dalan.net:19876    dlUserOrder dalan   collect_metric
//    bjc-tjlog.ijunhai.net:19876 sdkClick


    public static void main(String[] args) throws InterruptedException {
        System.setProperty("spark.scheduler.mode", "FAIR");
        Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
//        bjc-tjlog.ijunhai.net:19876 apiClick 60
        final String nameServer = "bjc-tjlog.ijunhai.net:19876";

        String second = "60";
        final List<String> topics = new ArrayList<String>();
//        topics.add("collect_metric");
        topics.add("sdkClick");
        Map<String, String> optionParams = new HashMap<String, String>();
        optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, nameServer);
//        optionParams.put(RocketMQConfig.PULL_TIMEOUT_MS,"60000");
//        optionParams.put(RocketMQConfig.CONSUMER_GROUP,"testOrder");
        String uuid = UUID.randomUUID().toString();
        String appName = nameServer + ":AdvClick";
        SparkConf sparkConf = new SparkConf().setAppName(appName)
                .setMaster("local[*]")
                ;
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(Integer.parseInt(second) * 1000));
        Broadcast kafkaSinkBroadcast = sc.sparkContext().broadcast(KafkaSink.apply(Save2Kafka.brokers()));
        Broadcast redisSinkBroadcast = sc.sparkContext().broadcast(RedisSinkAlone.apply());
//        Broadcast DlGreenPlumSink = sc.sparkContext().broadcast(GreenPlumSink.apply(ClickProcess.DL_DATABASE()));
//        Broadcast CmGreenPlumSink = sc.sparkContext().broadcast(GreenPlumSink.apply(ClickProcess.CM_DATABASE()));
        Broadcast gpSink = sc.sparkContext().broadcast(GreenPlumSink.apply(Monitor.database()));

        Broadcast detialGreenPlumSink = sc.sparkContext().broadcast(GreenPlumSink.apply(ClickProcess.DETIAL_DATABASE()));
        List<Broadcast<GreenPlumSink>> gpSinkList = new ArrayList<Broadcast<GreenPlumSink>>();
//        gpSinkList.add(DlGreenPlumSink);
//        gpSinkList.add(CmGreenPlumSink);
        gpSinkList.add(detialGreenPlumSink);



        LocationStrategy locationStrategy = LocationStrategy.PreferConsistent();
//        JedisCluster redisConn= RedisClient.getInstatnce().getJedis();
        Broadcast redisSinkCluster = sc.sparkContext().broadcast(RedisSink.apply());
//        assert redisConn != null;
        Map<MessageQueue, Object> queueToOffset = new HashMap<MessageQueue, Object>();

        JavaInputDStream<MessageExt> dStream;

            dStream = RocketMqUtils.createJavaMQPullStream(sc, GROUP, topics,
                    ConsumerStrategy.lastest(), false, false, true, locationStrategy, optionParams);


        final AtomicReference<Map<TopicQueueId, OffsetRange[]>> offsetRanges = new AtomicReference<Map<TopicQueueId, OffsetRange[]>>();


        dStream.transform(new Function<JavaRDD<MessageExt>, JavaRDD<MessageExt>>() {
            public JavaRDD<MessageExt> call(JavaRDD<MessageExt> v1) throws Exception {
                Map<TopicQueueId, OffsetRange[]> offsets = ((HasOffsetRanges) v1.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return v1;
            }
        }).foreachRDD(new VoidFunction<JavaRDD<MessageExt>>() {
            public void call(JavaRDD<MessageExt> messageExtJavaRDD) throws Exception {
                CommonProcess.extra(messageExtJavaRDD.rdd(), gpSink, "alhk.hw.net:19876", "haiwai");
//                ClickProcess.monitorTest(messageExtJavaRDD.rdd(), kafkaSinkBroadcast,
//                        "", "haiwai", topics, GROUP);
                //                MqClickProcess.processTest(messageExtJavaRDD.rdd(), kafkaSinkBroadcast,gpSinkList,
//                        topicsStr, offsetRanges.get(), nameServer, topics, GROUP);
            }
        });
        sc.start();
        sc.awaitTermination();

    }
}
