package consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

import java.nio.charset.Charset;

/**
 * @author xiantao.xiang
 * @date 2022-01-05 14:51
 **/
public class Tag1Consumer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("single_consumer_group_4");
        defaultMQPushConsumer.setNamesrvAddr("localhost:9876");
        defaultMQPushConsumer.subscribe("TopicTest", "TAG1");
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(messageExt -> {
                System.out.println(new String(messageExt.getBody(), Charset.defaultCharset()));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        defaultMQPushConsumer.start();
        Thread.sleep(10 * 60 * 1000);
    }
}
