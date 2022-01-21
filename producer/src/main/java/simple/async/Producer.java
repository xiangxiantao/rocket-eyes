package simple.async;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * @author xiantao.xiang
 * @date 2022-01-05 11:23
 **/
public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        final DefaultMQProducer producer = new DefaultMQProducer("sync_producer");
        producer.setNamesrvAddr("localhost:9876");
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.start();

        CountDownLatch countDownLatch = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            final Message message = new Message("TopicTest", "hello".getBytes(StandardCharsets.UTF_8));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                    countDownLatch.countDown();
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println(throwable);
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        System.out.println("ok");
        producer.shutdown();

    }
}
