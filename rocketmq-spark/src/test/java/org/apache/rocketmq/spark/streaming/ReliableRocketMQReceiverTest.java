/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.spark.streaming;

import com.deal;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spark.RocketMQConfig;
import org.apache.rocketmq.spark.RocketMQServerMock;
import org.apache.rocketmq.spark.RocketMqUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class ReliableRocketMQReceiverTest {
    private static RocketMQServerMock mockServer = new RocketMQServerMock();

    private static final String NAMESERVER_ADDR = "192.168.137.102:9876";
//    private static final String NAMESERVER_ADDR = mockServer.getNameServerAddr();
    private static final String CONSUMER_GROUP = "TagA";
    private static final String CONSUMER_TOPIC = "hello";

    @BeforeClass
    public static void start() throws Exception {
        mockServer.startupServer();
        System.out.println("");
        Thread.sleep(2000);

        //prepare data
        mockServer.prepareDataTo(CONSUMER_TOPIC, 5);
    }

    @AfterClass
    public static void stop() {
        mockServer.shutdownServer();
    }

    @Test
    public void testReliableRocketMQReceiver() {
        System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0");
        System.setProperty("spark.scheduler.mode", "FAIR");
        Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        Properties properties = new Properties();
        properties.setProperty(RocketMQConfig.NAME_SERVER_ADDR, NAMESERVER_ADDR);
        properties.setProperty(RocketMQConfig.CONSUMER_GROUP, CONSUMER_GROUP);
        properties.setProperty(RocketMQConfig.CONSUMER_TOPIC, CONSUMER_TOPIC);
        JavaInputDStream ds = RocketMqUtils.createJavaReliableMQPushStream(jssc, properties, StorageLevel.MEMORY_ONLY());
        ds.foreachRDD(new VoidFunction<JavaRDD<MessageExt>>() {
            @Override
            public void call(JavaRDD<MessageExt> messageExtJavaRDD) throws Exception {
                deal.process(messageExtJavaRDD.rdd());
            }
        });
        jssc.start();
        try {
            jssc.awaitTerminationOrTimeout(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
