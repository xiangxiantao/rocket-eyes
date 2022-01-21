package filter.tag;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * @author xiantao.xiang
 * @date 2022-01-21 14:54
 **/
public class MessageProducerWithTag {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        final DefaultMQProducer producer = new DefaultMQProducer("producer_group_1");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            int tagIndex = i % 2;
            final Message message = new Message("TopicTest", "TAG" + tagIndex, "tag hello".getBytes(StandardCharsets.UTF_8));
            final SendResult result = producer.send(message);
            System.out.println(result);
        }
        producer.shutdown();

    }
}
