package io.github.shawn.octopus.fluxus.engine.connector.source.rabbitmq;

import com.rabbitmq.client.*;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Test;

public class RabbitClientTests {
  private static final String brokerServiceUrl = "192.168.53.133";
  private static final int port = 5672;

  @Test
  public void testProducer() throws Exception {

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(brokerServiceUrl);
    connectionFactory.setPort(port);
    connectionFactory.setUsername("admin");
    connectionFactory.setPassword("admin");
    Connection connection;
    Channel channel;
    try {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
    } catch (Exception e) {
      throw new DataWorkflowException(e);
    }
    String queueName = "myQueue"; // 队列名称
    boolean durable = false; // 持久化标志位，true表示队列在重启时保留
    channel.queueDeclare(queueName, durable, false, false, null);
    List<String> messages = new ArrayList<>();
    int num = 1000;
    for (int i = 0; i < num; i++) {
      Item item =
          Item.builder()
              .id(RandomUtils.nextLong())
              .name(RandomStringUtils.randomAlphabetic(6))
              .goods(
                  JsonUtils.toJson(
                          Arrays.asList(
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build(),
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build(),
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build()))
                      .get())
              .build();
      String s = JsonUtils.toJson(item).get();
      messages.add(s);
    }
    for (String message : messages) {
      String exchangeName = ""; // 交换机名称，若未指定则使用默认的空字符串""
      String routingKey = queueName; // 路由键，此处直接使用队列名称作为路由键
      channel.basicPublish(
          exchangeName,
          routingKey,
          MessageProperties.PERSISTENT_TEXT_PLAIN,
          message.getBytes(StandardCharsets.UTF_8));
      System.out.println("Message sent to the queue.");
    }
    TimeUnit.SECONDS.sleep(1);
    channel.close();
    connection.close();
  }

  @Test
  public void testConsumer() throws Exception {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(brokerServiceUrl);
    connectionFactory.setPort(port);
    connectionFactory.setUsername("admin");
    connectionFactory.setPassword("admin");
    Connection connection;
    Channel channel;
    try {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
    } catch (Exception e) {
      throw new DataWorkflowException(e);
    }
    String queueName = "myQueue"; // 队列名称
    int i = 0;
    List<Long> tags = new ArrayList<>(100);
    try {
      while (true) {
        GetResponse response = channel.basicGet(queueName, false);
        if (response == null) {
          break;
        }
        byte[] body = response.getBody();
        long deliveryTag = response.getEnvelope().getDeliveryTag();
        tags.add(deliveryTag);
        System.out.printf(
            "consumer name: %s, topic: %s,  message_id: %s, message: %s\n",
            channel.getChannelNumber(),
            queueName,
            deliveryTag,
            new String(body, StandardCharsets.UTF_8));
        if (i == 99) {
          channel.basicAck(deliveryTag, true);
          i = 0;
          tags.clear();
        } else {
          i++;
        }
      }
    } catch (Exception e) {
      for (Long tag : tags) {
        channel.basicNack(tag, true, true);
      }
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConsumerListener() throws Exception {
    ClientBuilder builder = PulsarClient.builder().serviceUrl(brokerServiceUrl);
    PulsarClient client = builder.build();
    ConsumerImpl<byte[]> consumer =
        (ConsumerImpl<byte[]>)
            client
                .newConsumer()
                .topic("pulsar_callback_test")
                .subscriptionName("pulsar_callback_test_listener")
                .subscriptionType(
                    SubscriptionType
                        .Shared) // 指定消费模式，包含：Exclusive，Failover，Shared，Key_Shared。默认Exclusive模式
                .subscriptionInitialPosition(
                    SubscriptionInitialPosition.Earliest) // 指定从哪里开始消费，还有Latest，valueof可选，默认Latest
                .negativeAckRedeliveryDelay(
                    60, TimeUnit.SECONDS) // 指定消费失败后延迟多久broker重新发送消息给consumer，默认60s
                .batchReceivePolicy(
                    BatchReceivePolicy.builder()
                        .maxNumMessages(1000)
                        .maxNumBytes(1024 * 1024)
                        .timeout(500, TimeUnit.MILLISECONDS)
                        .build())
                //                                .intercept(new MyPulsarConsumerInterceptor())
                .messageListener(
                    (MessageListener<byte[]>)
                        (consumer1, msg) ->
                            System.out.printf(
                                "consumer name: %s, topic: %s, subscription: %s, message_id: %s, message: %s\n",
                                consumer1.getConsumerName(),
                                consumer1.getTopic(),
                                consumer1.getSubscription(),
                                ((MessageIdImpl) msg.getMessageId()).getLedgerId(),
                                new String(msg.getValue(), StandardCharsets.UTF_8)))
                .subscribe();
    while (true) {}
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Item {
    private long id;
    private String name;
    private String goods;
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Good {
    private long id;
    private String name;
  }

  @Test
  public void generate() {
    for (int i = 0; i < 1000; i++) {
      Item item =
          Item.builder()
              .id(RandomUtils.nextLong())
              .name(RandomStringUtils.randomAlphabetic(6))
              .goods(
                  JsonUtils.toJson(
                          Arrays.asList(
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build(),
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build(),
                              Good.builder()
                                  .id(RandomUtils.nextLong())
                                  .name(RandomStringUtils.randomAlphabetic(6))
                                  .build()))
                      .get())
              .build();
      String s = JsonUtils.toJson(item).get();
      System.out.println(s);
    }
  }
}
