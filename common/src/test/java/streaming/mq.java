package streaming;

import com.ijunhai.storage.redis.RedisSinkCluster;
import com.ijunhai.common.offset.redisRocketMQOffset;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.*;

/**
 * Created by admin on 2018/1/25.
 */
public class mq {
    public static void main(String[] args) {
        RedisSinkCluster redisSinkCluster = RedisSinkCluster.apply();
        List<String> topics = new ArrayList<>(Arrays.asList("haiwai","collect_metric"));
        Map<MessageQueue, Object> queueToOffset=new HashMap<MessageQueue, Object>();
        for (String topic:topics){
            queueToOffset.putAll(redisRocketMQOffset.readOffsetTest("alhk.hw.net:19876",topic,"Mq2Kafka",redisSinkCluster));
        }
        System.out.println(queueToOffset.toString());

    }


}
