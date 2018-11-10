package com.jcloud.jcq.sdk.demo;

import com.jcloud.jcq.protocol.Message;
import com.jcloud.jcq.sdk.JCQClientFactory;
import com.jcloud.jcq.sdk.auth.UserCredential;
import com.jcloud.jcq.sdk.producer.GlobalOrderProducer;
import com.jcloud.jcq.sdk.producer.ProducerConfig;
import com.jcloud.jcq.sdk.producer.model.SendBatchResult;
import com.jcloud.jcq.sdk.producer.model.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 全局顺序消息生产者 demo.
 * @date 2018-05-17
 */
public class GlobalOrderProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(GlobalOrderProducerDemo.class);
    /**
     * 用户accessKey
     */
    private static final String ACCESS_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA0";
    /**
     * 用户secretKey
     */
    private static final String SECRET_KEY = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB0";
    /**
     * 元数据服务器地址
     */
    private static final String META_SERVER_ADDRESS = "127.0.0.1:18888";
    /**
     * topic名称
     */
    private static final String TOPIC = "testTopic";

    public static void main(String[] args) throws Exception {
        // 创建全局顺序消息producer
        UserCredential userCredential = new UserCredential(ACCESS_KEY, SECRET_KEY);
        ProducerConfig producerConfig = ProducerConfig.builder()
                .metaServerAddress(META_SERVER_ADDRESS)
                .build();
        GlobalOrderProducer globalOrderProducer = JCQClientFactory.getInstance().createGlobalOrderProducer(userCredential, producerConfig);

        // 开启producer
        globalOrderProducer.start();

        // 创建message, 全局顺序消息不支持tag及延迟投递属性设置
        Message message = new Message();
        message.setTopic(TOPIC);
        message.setBody(("this is message boy").getBytes());
        Message message1 = new Message();
        message1.setTopic(TOPIC);
        message1.setBody(("this is message1 boy").getBytes());

        // 同步发送单条消息
        SendResult sendResult = globalOrderProducer.sendMessage(message);
        logger.info("messageId:{}, resultCode:{}", sendResult.getMessageId(), sendResult.getResultCode());

        // 同步发送批量消息, 一批最多32条消息
        List<Message> messages = new ArrayList<>();
        messages.add(message);
        messages.add(message1);
        SendBatchResult sendBatchResult = globalOrderProducer.sendBatchMessage(messages);
        logger.info("messageIds:{}, resultCode:{}", sendBatchResult.getMessageIds(), sendBatchResult.getResultCode());
    }
}
