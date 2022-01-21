package consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

import java.nio.charset.Charset;

/**
 * 这个需要broker开启对property的支持
 * 在broker.properties文件中添加配置 enableProperty = true
 *
 * @author xiantao.xiang
 * @date 2022-01-21 15:26
 **/
public class PropertiesConsumer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("single_consumer_group_3");
        defaultMQPushConsumer.setNamesrvAddr("localhost:9876");
        defaultMQPushConsumer.subscribe("TopicTest", MessageSelector.bySql("a between 0 and 5"));
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(messageExt -> {
                System.out.println(messageExt.getProperty("a"));
                System.out.println(messageExt.getProperty("b"));
                System.out.println(new String(messageExt.getBody(), Charset.defaultCharset()));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        defaultMQPushConsumer.start();
        Thread.sleep(10 * 60 * 1000);
    }
}
