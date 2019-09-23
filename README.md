# 消息队列JCQ Java SDK使用示例 #

# 环境准备 #
请确保已经安装了 JDK 环境, JCQ Java SDK适用于jdk7及以上版本。

# SDK使用方法 #
建议使用Maven 方式引入依赖：
```xml
        <dependency>
            <groupId>com.jdcloud</groupId>
            <artifactId>jcq-java-sdk</artifactId>
            <version>1.3.0</version>
        </dependency>
```

# demo说明 #
1. 普通消息生产者：    参考ProducerDemo
2. 全局顺序消息生产者：参考GlobalOrderProducerDemo
3. 订阅型消费者：      参考ConsumerDemo
4. 拉取型消费者：      参考PullConsumerDemo