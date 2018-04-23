package com.ijunhai.common.rocketmq;


import com.ijunhai.common.serde.MessageSerde;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Admin on 2017-08-02.
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        List<Thread> threads = new ArrayList<Thread>();
        int size = 1;
        final DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
//        producer.setNamesrvAddr("123.59.62.55:9876");
//        producer.setNamesrvAddr("123.59.84.230:19876");
        producer.setNamesrvAddr("bjc-apilog.dalan.net:19876");
        //Launch the instance.
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        String sb = "";
        int i = 0;
        int j = 0;
        FileReader reader = new FileReader("C:\\Users\\Admin\\Desktop\\data\\Agent_order_17_09_13.log");
        BufferedReader br = new BufferedReader(reader);
        String str = null;
        String buf = "";
        String content = "NORMAL: [2017-11-06 00:02:34] CIP[123.58.9.168] AD_CLICK[{\"junhai_adid\":\"15154\",\"game\":\"U200000048\",\"channel\":\"jinritoutiao\",\"device_id\":\"61AF1239-36ED-4736-885F-5816B3E1A1CC\",\"ad_id\":\"74153213600\",\"ud_id\":\"\",\"mac\":\"e3f5536a141811db40efd6400f1d0a4e\",\"ext1\":\"\",\"ext2\":\"\",\"callback_url\":\"http:\\/\\/ad.toutiao.com\\/track\\/activate\\/?callback=CKD9gJ-UAhDC1_iflAIYhvuBkAwghp7viYEBKLGz1JuUAjAMOA1CIDIwMTcxMTA2MDAwMjAxMDEwMDEwMDYxMTM2NTQ5ODE5SAs=&os=1&muid=61AF1239-36ED-4736-885F-5816B3E1A1CC\",\"click_time\":\"2017-11-06 00:02:34\"}]";
        while ((str = br.readLine()) != null) {
            sb += str + "\n";

//            Message msg1 = new Message("agent" /* Topic */,
//                    "a" /* Tag */,
//                    str.getBytes());
            ByteBuffer bb = ByteBuffer.wrap(sb.getBytes());
            i++;
            if (i > 200) {
                bb.position(sb.getBytes().length);
                Message msg1 = MessageSerde.serialize("test4", "127.0.0.1", "file", bb, i, "dalan");
//                Message msg1=new Message("sdk",content.getBytes());
                try {
                    producer.send(msg1);
                } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                    e.printStackTrace();
                }
//                sb="";
                break;
            }
        }
        producer.shutdown();
        br.close();
        reader.close();
//        for (int i = 0; i < size; i++) {
//            Thread thread=new Thread(new Runnable() {
//                public void run() {
//                    //Instantiate with a Producer group name.
//                    int temp=0;
//                    for (int j = temp; j < temp+1; j++) {
//                        //Create a message instance, specifying topic, tag and message body.
////                        Message msg1 = new Message("sdk" /* Topic */,
////                                "a" /* Tag */,
////                                ("{\"binlogFilename\":\"mysql-bin.000010\",\"xid\":40,\"nextPosition\":998,\"rows\":[{\"database\":\"youyun_agent\",\"data\":{\"server\":1,\"goods_name\":\"60钻石\",\"game_role_name\":\"黄唯香\",\"device_id\":\"00000000-0281-5e60-cd00-4df762cce3ff\",\"create_time\":\"2014-11-13 04:16:57\",\"channel_trade_sn\":\"1411125502598099941\",\"goods_id\":\"10002\",\"goods_count\":1,\"game_role_id\":\"2\",\"pay_status\":1,\"update_time\":1415781152000,\"game_url\":\"http://pay.qihoo.nzsgz.com/api/qihoo/RevPayNotify.php\",\"money\":6,\"user_id\":\"161763114\",\"cp_trade_sn\":\"4411415776617177qihoo_android\",\"pay_type\":\"1\",\"id\":1,\"channel_id\":101232,\"order_sn\":2017032431224822135,\"status\":4,\"game_id\":72},\"type\":\"UPDATE\",\"table\":\"agent_order\"}]}").getBytes() /* Message body */
////                        );
////                        Message msg2 = new Message("sdk" /* Topic */,
////                                "a" /* Tag */,
////                                ("{\"binlogFilename\":\"mysql-bin.000010\",\"xid\":40,\"nextPosition\":998,\"rows\":[{\"database\":\"youyun_agent\",\"data\":{\"channel_id\":199,\"order_sn\":2017032431224822135,\"status\":4,\"game_id\":1,\"create_ip\":\"39.128.23.215\"},\"type\":\"UPDATE\",\"table\":\"agent_order_copy\"}]}").getBytes() /* Message body */
////                        );
//                        Message msg1 = new Message("sdk" /* Topic */,
//                                "a" /* Tag */,
//                                ("{\"binlogFilename\":\"mysql-bin.000010\",\"xid\":40,\"nextPosition\":998,\"rows\":[{\"database\":\"youyun_agent\",\"data\":{\"server\":1,\"goods_name\":\"60钻石\",\"game_role_name\":\"黄唯香\",\"device_id\":\"00000000-0281-5e60-cd00-4df762cce3ff\",\"create_time\":\"2014-11-13 04:16:57\",\"channel_trade_sn\":\"1411125502598099941\",\"goods_id\":\"10002\",\"goods_count\":1,\"game_role_id\":\"2\",\"pay_status\":1,\"update_time\":1415781152000,\"game_url\":\"http://pay.qihoo.nzsgz.com/api/qihoo/RevPayNotify.php\",\"money\":6,\"user_id\":\"161763114\",\"cp_trade_sn\":\"4411415776617177qihoo_android\",\"pay_type\":\"1\",\"id\":1,\"channel_id\":13,\"order_sn\":2017032431224884674,\"status\":4,\"game_id\":55},\"type\":\"UPDATE\",\"table\":\"agent_order\"}]}").getBytes() /* Message body */
//                        );
//                        Message msg2 = new Message("sdk" /* Topic */,
//                                "a" /* Tag */,
//                                ("{\"binlogFilename\":\"mysql-bin.000010\",\"xid\":40,\"nextPosition\":998,\"rows\":[{\"database\":\"youyun_agent\",\"data\":{\"channel_id\":199,\"order_sn\":2017032431224884674,\"status\":4,\"game_id\":1,\"create_ip\":\"39.128.23.215\"},\"type\":\"UPDATE\",\"table\":\"agent_order_copy\"}]}").getBytes() /* Message body */
//                        );
//                        //Call send message to deliver message to one of brokers.
//                        try {
//                            producer.send(msg1);
//                            producer.send(msg2);
//                        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
//                            e.printStackTrace();
//                        }
////            System.out.printf("%s%n", sendResult);
//                    }
//                    //Shut down once the Producer instance is not longer in use.
//                    producer.shutdown();
//                }
//            });
//            threads.add(thread);
//        }
//        for (int i = 0; i < size; i++) {
//            threads.get(i).start();
//        }
    }
//    public static void main(String[] args){
//        DefaultMQProducer Producer = new DefaultMQProducer("Producer");
//        Producer.setNamesrvAddr("192.168.137.102:9876");
//        try {
//            Producer.start();
//
//            Message msg = new Message("BenchmarkTest",
//                    "push",
//                    "1",
//                    "Just for test.".getBytes());
//
//            SendResult result = Producer.send(msg);
//            System.out.println("id:" + result.getMsgId() +
//                    " result:" + result.getSendStatus());
//
//            msg = new Message("BenchmarkTest",
//                    "push",
//                    "2",
//                    "Just for test.".getBytes());
//
//            result = Producer.send(msg);
//            System.out.println("id:" + result.getMsgId() +
//                    " result:" + result.getSendStatus());
//
//            msg = new Message("PullTopic",
//                    "pull",
//                    "1",
//                    "Just for test.".getBytes());
//
//            result = Producer.send(msg);
//            System.out.println("id:" + result.getMsgId() +
//                    " result:" + result.getSendStatus());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }finally{
//            Producer.shutdown();
//        }
//    }
}
