package schedule;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * 延迟消息生产者
 *
 * @author xiantao.xiang
 * @date 2022-01-21 14:17
 **/
public class ScheduleMessageProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_group_1");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        final Message message = new Message("TopicTest", ("schedule 01 hello").getBytes(StandardCharsets.UTF_8));
        message.setDelayTimeLevel(3);
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("发送成功了");
            }

            @Override
            public void onException(Throwable e) {
                System.out.println("发送失败了");
            }
        });
    }
}
