package io.github.shawn.octopus.fluxus.engine.connector.source.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.io.IOException;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

/** @author zuoyl */
@Slf4j
public class RabbitmqSource implements Source<RabbitmqSourceConfig> {

  private final RabbitmqSourceConfig config;
  private final RabbitmqSourceConfig.RabbitmqSourceOptions options;

  private RabbitmqMessageConverter converter;

  //  private final AtomicInteger commitSize = new AtomicInteger(0);

  private Connection connection;
  private Channel channel;
  private volatile Long tag;
  private final Counter counter;

  public RabbitmqSource(RabbitmqSourceConfig config) {
    this.config = config;
    this.options = config.getOptions();
    counter =
        Counter.builder("rabbitmq-source-flush-counter")
            .tags(
                Arrays.asList(
                    Tag.of("source_Id", config.getId()), Tag.of("source_name", config.getName())))
            .register(Metrics.globalRegistry);
  }

  @Override
  public void init() throws StepExecutionException {
    log.info("rabbitmq source {} init beginning...", config.getId());
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(options.getHost());
    connectionFactory.setPort(options.getPort());
    connectionFactory.setUsername(options.getUserName());
    connectionFactory.setPassword(options.getPassword());
    connectionFactory.setConnectionTimeout(options.getConnectionTimeout());
    if (StringUtils.isNotEmpty(options.getVirtualhost())) {
      connectionFactory.setVirtualHost(options.getVirtualhost());
    }
    try {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
      if (StringUtils.isNotBlank(options.getExchangeName())) {
        channel.queueDeclare(options.getQueueName(), true, false, false, null);
        channel.queueBind(
            options.getQueueName(), options.getExchangeName(), options.getRoutingKey());
      }
      log.info("rabbitmq source {} init success...", config.getId());
    } catch (Exception e) {
      log.error("rabbitmq source {} init error...", config.getId(), e);
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void dispose() throws StepExecutionException {
    log.info("rabbitmq source {} close resources beginning...", config.getId());
    try {
      if (channel != null) {
        channel.close();
      }
      if (connection != null) {
        connection.close();
      }
      log.info("rabbitmq source {} close resources success...", config.getId());
    } catch (Exception e) {
      log.error("rabbitmq source {} close resources error...", config.getId(), e);
      throw new StepExecutionException(
          String.format("rabbitmq source %s close resources error...", config.getId()), e);
    }
  }

  @Override
  public RowRecord read() throws StepExecutionException {
    try {
      GetResponse response = channel.basicGet(options.getQueueName(), false);
      if (response == null) {
        return null;
      }
      tag = response.getEnvelope().getDeliveryTag();
      counter.increment();
      return converter.convert(response.getBody());
    } catch (Exception e) {
      throw new StepExecutionException("rabbitmq consumer receive error.", e);
    }
  }

  @Override
  public boolean commit() throws StepExecutionException {
    try {
      if (tag == null) {
        return true;
      }
      log.info("ack,count:" + counter.count());
      channel.basicAck(tag, true);
    } catch (IOException e) {
      throw new StepExecutionException("rabbitmq consumer commit error.", e);
    }
    return true;
  }

  @Override
  public void abort() throws StepExecutionException {}

  @Override
  public <S> void setConverter(RowRecordConverter<S> converter) {
    this.converter = (RabbitmqMessageConverter) converter;
  }

  @Override
  public RabbitmqSourceConfig getSourceConfig() {
    return config;
  }
}
