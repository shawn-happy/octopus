package io.github.shawn.octopus.fluxus.engine.connector.sink.pulsar;

import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.api.serialization.SerializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.serialization.CSVSerializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.serialization.JsonSerializationSchema;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;

@Slf4j
public class PulsarSink implements Sink<PulsarSinkConfig> {

  private final PulsarSinkConfig config;
  private final PulsarSinkConfig.PulsarSinkOptions options;
  private PulsarClient pulsarClient;
  private Producer<byte[]> producer;
  private final AtomicLong pendingMessages;
  private SerializationSchema serializationSchema;

  public PulsarSink(PulsarSinkConfig config) {
    this.config = config;
    this.options = config.getOptions();
    pendingMessages = new AtomicLong(0);
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
      producer =
          pulsarClient
              .newProducer()
              .producerName(options.getProducer())
              .blockIfQueueFull(true)
              .topic(options.getTopic())
              .compressionType(CompressionType.LZ4)
              .sendTimeout(0, TimeUnit.SECONDS)
              .create();
      log.info("pulsar sink {} init success", config.getId());
    } catch (Exception e) {
      log.error("pulsar sink {} init error", config.getId(), e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void write(RowRecord source) throws StepExecutionException {
    if (source == null) {
      return;
    }
    String[] fieldNames = source.getFieldNames();
    DataWorkflowFieldType[] fieldTypes = source.getFieldTypes();

    if (serializationSchema == null) {
      PulsarSinkFormat format = options.getFormat();
      if (format == PulsarSinkFormat.JSON) {
        serializationSchema = new JsonSerializationSchema(new RowFieldType(fieldNames, fieldTypes));
      } else if (format == PulsarSinkFormat.CSV) {
        serializationSchema =
            new CSVSerializationSchema(
                new RowFieldType(fieldNames, fieldTypes),
                options.getDelimiter(),
                options.getArrayElementDelimiter());
      } else {
        throw new StepExecutionException("unsupported pulsar sink format: " + format.name());
      }
    }
    try {
      if (source instanceof SourceRowRecord) {
        byte[] serialize = serializationSchema.serialize(source);
        TypedMessageBuilder<byte[]> typedMessageBuilder =
            new TypedMessageBuilderImpl<>((ProducerBase<byte[]>) producer, Schema.BYTES);
        typedMessageBuilder.value(serialize);
        pendingMessages.incrementAndGet();
        typedMessageBuilder.sendAsync();
      } else if (source instanceof TransformRowRecord) {
        TransformRowRecord transformRowRecord = (TransformRowRecord) source;
        RowRecord record;
        while ((record = transformRowRecord.pollNextRecord()) != null) {
          byte[] serialize = serializationSchema.serialize(record);
          TypedMessageBuilder<byte[]> typedMessageBuilder =
              new TypedMessageBuilderImpl<>((ProducerBase<byte[]>) producer, Schema.BYTES);
          typedMessageBuilder.value(serialize);
          pendingMessages.incrementAndGet();
          typedMessageBuilder.sendAsync();
        }
      }
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void dispose() throws StepExecutionException {
    log.info("pulsar sink {} begin close resources", config.getId());
    try {
      if (producer != null && producer.isConnected()) {
        producer.close();
      }
      if (pulsarClient != null && !pulsarClient.isClosed()) {
        pulsarClient.close();
      }
      producer = null;
      pulsarClient = null;
    } catch (Exception e) {
      throw new StepExecutionException(
          String.format("pulsar sink %s close resources error.", config.getId()), e);
    }
    log.info("pulsar sink {} close resources success", config.getId());
  }

  @Override
  public PulsarSinkConfig getSinkConfig() {
    return config;
  }

  @Override
  public boolean commit() throws StepExecutionException {
    try {
      if (pendingMessages.longValue() > 0) {
        producer.flush();
        pendingMessages.set(0);
      }
      return true;
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void abort() throws StepExecutionException {}
}
