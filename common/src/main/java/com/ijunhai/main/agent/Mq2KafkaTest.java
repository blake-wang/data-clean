package com.ijunhai.main.agent;

import com.ijunhai.common.logsystem.Monitor;
import com.ijunhai.storage.greenplum.GreenPlumSink;
import com.ijunhai.storage.kafka.KafkaSink;
import com.ijunhai.storage.kafka.Save2Kafka;
import com.ijunhai.storage.redis.RedisSink;
import com.ijunhai.common.serde.MessageSerde;
import com.ijunhai.common.serde.WrapMessage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spark.ConsumerStrategy;
import org.apache.rocketmq.spark.LocationStrategy;
import org.apache.rocketmq.spark.RocketMQConfig;
import org.apache.rocketmq.spark.RocketMqUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;

import java.util.*;


public class Mq2KafkaTest {
    public static void main(String args[]) {
        Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Map<String, String> optionParams = new HashMap<>();
//海外  alhk.hw.net:19876             hwUserOrder haiwai  collect_metric
//大蓝  bjc-apilog.dalan.net:19876    dlUserOrder dalan   collect_metric
//        bjd-apilog.ijunhai.net
//        bjc-apilog.ijunhai.net
//        txtj-apilog.ijunhai.net
//    bjc-tjlog.ijunhai.net:19876 sdkClick
//广告点击  bjc-tjlog.ijunhai.net:19876 apiClick
        List<String> topics = new ArrayList<>();
        topics.add("sdkClick");
//        topics.add("collect_metric");

        optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, "bjc-tjlog.ijunhai.net:19876");
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(60000));
        Broadcast redisSinkCluster = sc.sparkContext().broadcast(RedisSink.apply());
        Broadcast kafkaSinkBroadcast = sc.sparkContext().broadcast(KafkaSink.apply(Save2Kafka.brokers()));
//        "collect_metric"
        LocationStrategy locationStrategy = LocationStrategy.PreferConsistent();
        String GROUP = "Mq2KafkaTest";
//        JavaInputDStream<MessageExt> stream = RocketMqUtils.createJavaMQPullStream(sc, UUID.randomUUID().toString(),
//                topics, ConsumerStrategy.lastest(), false, false, false, locationStrategy, optionParams);

//        Map<MessageQueue, Object> queueToOffset = new HashMap<MessageQueue, Object>();
//        for (String topic : topics) {
//            Map<String, String> init_offset_map = new HashMap();
//            init_offset_map.put("broker-a|0", "81705");
//            init_offset_map.put("broker-a|1", "81705");
//            init_offset_map.put("broker-a|2", "81705");
//            init_offset_map.put("broker-a|3", "81705");
//            if (!init_offset_map.isEmpty()) {
//                for (String brokerNameAndQueueId : init_offset_map.keySet()) {
//                    String[] split = brokerNameAndQueueId.split("\\|"); //0 是brokerName 1是queueID
//                    queueToOffset.put(new MessageQueue(topic, split[0], Integer.parseInt(split[1])), Long.parseLong(init_offset_map.get(brokerNameAndQueueId)));
//                }
//            }
//        }
//        Map queueToOffset =null;

        JavaInputDStream<MessageExt> stream;
//        if (!queueToOffset.isEmpty()) {
//            stream = RocketMqUtils.createJavaMQPullStream(sc, GROUP, topics,
//                    ConsumerStrategy.specificOffset(queueToOffset), false, true, true, locationStrategy, optionParams);
//        } else {
        stream = RocketMqUtils.createJavaMQPullStream(sc, GROUP, topics,
                ConsumerStrategy.lastest(), false, false, true, locationStrategy, optionParams);
//        }
        Broadcast GPSink = sc.sparkContext().broadcast(GreenPlumSink.apply(Monitor.database()));

        final Set<MessageExt> result = Collections.synchronizedSet(new HashSet<MessageExt>());
        LongAccumulator acc = sc.ssc().sparkContext().longAccumulator();

        stream.foreachRDD(new VoidFunction<JavaRDD<MessageExt>>() {
            public void call(JavaRDD<MessageExt> messageExtJavaRDD) throws Exception {
                messageExtJavaRDD.foreach(new VoidFunction<MessageExt>() {
                    public void call(MessageExt messageExt) throws Exception {
//                        CommonProcess.extra(messageExtJavaRDD.rdd(), GPSink,
//                                "alhk.hw.net:19876"
//                                , "haiwai");

//                        MqClickProcess.monitorTest(messageExtJavaRDD.rdd(), kafkaSinkBroadcast,
//                                "", "haiwai", topics, GROUP);


                        if (messageExt.getBody() != null) {


//                            System.out.println(new String(messageExt.getBody()));
                            WrapMessage wm = MessageSerde.deserialize(messageExt);
                            String[] msgs = new String(wm.getMessageBody()).split("\n");
                            String ip = wm.getIp();
                            String system = wm.getDataSystem();
                            String filePath = wm.getFilePath();
                            int lineCount = wm.getLineCount();

//                            System.out.println("lineCount:"+lineCount);
//                            System.out.println("msgs"+msgs.length);
//                            test.add(msgs.length);
//                            if(test.getcount()>2000) {
//                                System.out.println(new Date());
//                                System.out.println(test.getcount());
//                                test.set(0);
//                            }

//                            String outputString = "[";
//                            if (system.equals("")) {
//                                System.out.println(lineCount + "  " + ip + " # " + system + " # " + filePath + " # ");
//                            }
//                            Save2Kafka.saveMetrics(kafkaSinkBroadcast, "test", new Date(), "", "", "", "linecount", lineCount + "");
//                            System.out.println("system:"+new Date()+",ip:"+ip+",filePath:"+filePath+",lineCount:"+lineCount);

                            for (String msg : msgs) {
//                                int len=HelloSpark.getCounts(msg);
//                                length+=len;
//                                if (msg.toLowerCase().contains("order")) {

                                    System.out.println(msg);
////                                kafkaSinkBroadcast.value().send("agentLogin", msg);
//                                }
//                                acc.add(length);

                            }
//                            System.out.println(acc.value());
//                            System.out.println("length"+length);
                        }
                    }
                });

            }
        });

        sc.start();
        try {
            sc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
