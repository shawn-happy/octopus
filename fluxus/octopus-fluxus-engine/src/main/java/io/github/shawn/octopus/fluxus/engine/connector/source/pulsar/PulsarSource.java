package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

@Slf4j
public class PulsarSource implements Source<PulsarSourceConfig> {

  private final PulsarSourceConfig config;
  private final PulsarSourceConfig.PulsarSourceOptions options;
  private PulsarClient pulsarClient;
  private Consumer<byte[]> consumer;
  private volatile MessageId lastMessageId;
  private final ConsumerInterceptor<byte[]>[] interceptor =
      new ConsumerInterceptor[] {new PulsarConsumerInterceptor()};
  private PulsarMessageConverter convertor;
  private final Counter counter;

  public PulsarSource(PulsarSourceConfig config) {
    this.config = config;
    this.options = config.getOptions();
    counter =
        Counter.builder("pulsar-source-flush-counter")
            .tags(
                Arrays.asList(
                    Tag.of("source_Id", config.getId()), Tag.of("source_name", config.getName())))
            .register(Metrics.globalRegistry);
  }

  @Override
  public void init() throws StepExecutionException {
    ClientBuilder builder = null;
    try {
      builder = PulsarClient.builder().serviceUrl(options.getClientUrl());
      if (StringUtils.isNotBlank(options.getToken())) {
        builder = builder.authentication(AuthenticationFactory.token(options.getToken()));
      }
      pulsarClient = builder.build();

      SubscriptionInitialPosition position = null;
      if (StringUtils.isNotBlank(options.getSubscriptionType())) {
        if ("Earliest".equalsIgnoreCase(options.getSubscriptionType())) {
          position = SubscriptionInitialPosition.Earliest;
        } else if ("Latest".equalsIgnoreCase(options.getSubscriptionType())) {
          position = SubscriptionInitialPosition.Latest;
        }
      }
      consumer =
          pulsarClient
              .newConsumer()
              .topic(options.getTopic())
              .subscriptionName(options.getSubscription())
              .subscriptionInitialPosition(position)
              .consumerName(options.getSubscription())
              .intercept(interceptor)
              .subscribe();
      log.info("pulsar source {} init success", config.getId());
    } catch (Exception e) {
      log.error("pulsar source {} init error", config.getId(), e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void dispose() throws StepExecutionException {
    log.info("pulsar source {} begin close resources", config.getId());
    try {
      for (ConsumerInterceptor<byte[]> consumerInterceptor : interceptor) {
        consumerInterceptor.close();
      }
      if (consumer != null && consumer.isConnected()) {
        consumer.close();
      }
      if (pulsarClient != null && !pulsarClient.isClosed()) {
        pulsarClient.close();
      }
    } catch (Exception e) {
      throw new StepExecutionException(
          String.format("pulsar source %s close resources error.", config.getId()), e);
    }
    log.info("pulsar source {} close resources success", config.getId());
  }

  @Override
  public RowRecord read() throws StepExecutionException {
    try {
      Message<byte[]> message = consumer.receive(30, TimeUnit.SECONDS);
      if (message == null) {
        return null;
      }
      lastMessageId = message.getMessageId();
      counter.increment();
      return convertor.convert(message);
    } catch (Exception e) {
      throw new StepExecutionException("pulsar consumer receive error.", e);
    }
  }

  @Override
  public boolean commit() throws StepExecutionException {
    try {
      flush();
      return true;
    } catch (Exception e) {
      throw new StepExecutionException("pulsar consumer commit error.", e);
    }
  }

  @Override
  public void abort() throws StepExecutionException {
    try {
      consumer.negativeAcknowledge(lastMessageId);
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public <S> void setConverter(RowRecordConverter<S> convertor) {
    this.convertor = (PulsarMessageConverter) convertor;
  }

  @Override
  public PulsarSourceConfig getSourceConfig() {
    return config;
  }

  private void flush() throws Exception {
    if (lastMessageId == null) {
      return;
    }
    consumer.acknowledgeCumulative(lastMessageId);
  }
}
