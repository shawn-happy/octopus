package io.github.shawn.octopus.fluxus.engine.connector.source.activemq;

import io.github.shawn.octopus.fluxus.api.connector.Source;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.util.Arrays;
import javax.jms.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.commons.lang3.StringUtils;

/** @author zuoyl */
@Slf4j
public class ActivemqSource implements Source<ActivemqSourceConfig> {

  private final ActivemqSourceConfig config;
  private final ActivemqSourceConfig.ActivemqSourceOptions options;

  private Connection connection;
  private Session session;
  private ActiveMQMessageConsumer consumer;

  private ActivemqMessageConverter converter;
  //  private final AtomicInteger commitSize = new AtomicInteger(0);
  private final Counter counter;

  private static final String MQ_TYPE_TOPIC = "topic";

  public ActivemqSource(ActivemqSourceConfig config) {
    this.config = config;
    this.options = config.getOptions();
    counter =
        Counter.builder("activemq-source-flush-counter")
            .tags(
                Arrays.asList(
                    Tag.of("source_Id", config.getId()), Tag.of("source_name", config.getName())))
            .register(Metrics.globalRegistry);
  }

  @Override
  public void init() throws StepExecutionException {
    try {
      ActiveMQConnectionFactory activeMQConnectionFactory =
          new ActiveMQConnectionFactory(
              options.getUserName(), options.getPassword(), options.getBrokerUrl());
      connection = activeMQConnectionFactory.createConnection();
      connection.start();
      // 创建会话; 参数:  事务, 签收机制;
      session = connection.createSession(options.isTransacted(), options.getAcknowledgeMode());
      // 创建目的地;例如:队列 TODO TOPIC BUG
      Destination destination;
      if (StringUtils.equalsIgnoreCase(MQ_TYPE_TOPIC, options.getType())) {
        destination = session.createTopic(options.getQueueName());
      } else {
        destination = session.createQueue(options.getQueueName());
      }
      // 创建消费者;
      consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void dispose() throws StepExecutionException {
    try {
      if (consumer != null) {
        consumer.close();
      }
      if (session != null) {
        session.close();
      }
      if (connection != null) {
        connection.close();
      }
    } catch (Exception e) {
      throw new StepExecutionException("activemq connection close error.", e);
    }
  }

  @Override
  public RowRecord read() throws StepExecutionException {
    try {
      Message message = consumer.receive(2000);
      if (message == null) {
        return null;
      }
      String msg = ((TextMessage) message).getText();
      counter.increment();
      return converter.convert(msg);
    } catch (Exception e) {
      throw new StepExecutionException("activemq consumer receive error.", e);
    }
  }

  @Override
  public boolean commit() throws StepExecutionException {
    try {
      log.info("ack,count:" + counter.count());
      consumer.acknowledge();
      return true;
    } catch (Exception e) {
      throw new StepExecutionException("pulsar consumer commit error.", e);
    }
  }

  @Override
  public void abort() throws StepExecutionException {
    // TODO
  }

  @Override
  public <S> void setConverter(RowRecordConverter<S> converter) {
    this.converter = (ActivemqMessageConverter) converter;
  }

  @Override
  public ActivemqSourceConfig getSourceConfig() {
    return config;
  }
}
