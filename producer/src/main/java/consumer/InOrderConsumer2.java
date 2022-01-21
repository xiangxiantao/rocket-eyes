package consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.Charset;
import java.util.List;

/**
 * 顺序消息的消费者
 *
 * @author xiantao.xiang
 * @date 2022-01-21 13:33
 **/
public class InOrderConsumer2 {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("consumer_group_2");
        defaultMQPushConsumer.setNamesrvAddr("localhost:9876");
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        defaultMQPushConsumer.subscribe("TopicTest", "*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                msgs.forEach(messageExt -> {
                    System.out.println(Thread.currentThread().getName() + "===" + messageExt.getQueueId() + "===" + new String(messageExt.getBody(), Charset.defaultCharset()));
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        defaultMQPushConsumer.start();
        Thread.sleep(10 * 60 * 1000);

    }
}
