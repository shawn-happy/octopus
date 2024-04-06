package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

@Slf4j
public class PulsarConsumerInterceptor<T> implements ConsumerInterceptor<T> {

  @Override
  public void close() {}

  @Override
  public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
    return message;
  }

  @Override
  public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {
    if (exception != null) {
      log.error(
          "message receive error. topic: {}, consumer name: {}, subscription name: {}, message id: {}",
          consumer.getTopic(),
          consumer.getConsumerName(),
          consumer.getSubscription(),
          ((MessageIdImpl) messageId).getLedgerId());
    }
  }

  @Override
  public void onAcknowledgeCumulative(
      Consumer<T> consumer, MessageId messageId, Throwable exception) {
    if (exception != null) {
      log.error(
          "message receive error. topic: {}, consumer name: {}, subscription name: {}, message id: {}",
          consumer.getTopic(),
          consumer.getConsumerName(),
          consumer.getSubscription(),
          ((MessageIdImpl) messageId).getLedgerId());
    }
  }

  @Override
  public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> messageIds) {
    log.error(
        "message receive error. topic: {}, consumer name: {}, subscription name: {}, message id: {}",
        consumer.getTopic(),
        consumer.getConsumerName(),
        consumer.getSubscription(),
        messageIds
            .stream()
            .map(messageId -> (MessageIdImpl) messageId)
            .map(MessageIdImpl::getLedgerId)
            .map(String::valueOf)
            .collect(Collectors.joining(",")));
  }

  @Override
  public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> messageIds) {
    log.error(
        "message receive timeout. topic: {}, consumer name: {}, subscription name: {}, message id: {}",
        consumer.getTopic(),
        consumer.getConsumerName(),
        consumer.getSubscription(),
        messageIds
            .stream()
            .map(messageId -> (MessageIdImpl) messageId)
            .map(MessageIdImpl::getLedgerId)
            .map(String::valueOf)
            .collect(Collectors.joining(",")));
  }
}
