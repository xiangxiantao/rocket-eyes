package transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * 事务消息的生产者
 *
 * @author xiantao.xiang
 * @date 2022-01-21 16:43
 **/
public class TransactionMessageProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        final TransactionMQProducer transactionMQProducer = new TransactionMQProducer("transaction_produce");
        transactionMQProducer.setNamesrvAddr("localhost:9876");
        transactionMQProducer.setTransactionListener(new TransactionListener() {
            /**
             * 消息发送之后，broker会调用这个接口执行本地的事务，并且返回本地事务的提交状态
             * 如果返回COMMIT_MESSAGE 说明本地事务已经执行完毕，broker会正式将消息落盘并push到consumer
             * 如果返回UNKNOW，broker会稍后通过checkLocalTransaction方法再次检查事务状态
             * 如果返回ROLLBACK_MESSAGE，说明本地事务执行失败了，需要回滚消息，最终消息不会发送到consumer
             * @param msg
             * @param arg
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("executeLocalTransaction");
                return LocalTransactionState.UNKNOW;
            }

            /**
             * 当executeLocalTransaction返回UNKNOW时，broker会定期调用该接口查询本地事务的最终结果
             * broker默认会检查15次，超过次数之后会将消息抛弃并打印错误日志
             * @param msg
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("checkLocalTransaction");
                final String tags = msg.getTags();
                if (Integer.parseInt(tags) % 2 == 0) {
                    System.out.println("COMMIT_MESSAGE");
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else {
                    System.out.println("ROLLBACK_MESSAGE");
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }
        });
        transactionMQProducer.setExecutorService(Executors.newSingleThreadScheduledExecutor());
        transactionMQProducer.start();
        for (int i = 0; i < 10; i++) {
            final Message message = new Message("TopicTest", String.valueOf(i), "transaction4 hello".getBytes(StandardCharsets.UTF_8));
            final TransactionSendResult transactionSendResult = transactionMQProducer.sendMessageInTransaction(message, null);
            System.out.println("消息发送成功，当前state" + transactionSendResult.getLocalTransactionState());
        }
        Thread.sleep(10 * 60 * 1000);
        transactionMQProducer.shutdown();
    }
}
