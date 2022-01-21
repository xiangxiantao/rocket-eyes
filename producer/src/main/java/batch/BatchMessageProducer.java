package batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 批量消息生产者
 *
 * @author xiantao.xiang
 * @date 2022-01-21 14:31
 **/
public class BatchMessageProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        final DefaultMQProducer producer = new DefaultMQProducer("producer_group_1");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            final Message message = new Message("TopicTest", ("batch hello" + i).getBytes(StandardCharsets.UTF_8));
            messages.add(message);
        }
        producer.send(messages);
        producer.shutdown();
    }
}
