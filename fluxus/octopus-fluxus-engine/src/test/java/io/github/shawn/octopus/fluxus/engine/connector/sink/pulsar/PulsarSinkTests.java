package io.github.shawn.octopus.fluxus.engine.connector.sink.pulsar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class PulsarSinkTests {
  @Test
  public void testPulsarSink() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/pulsar/pulsar-sink.json")),
            StandardCharsets.UTF_8);
    PulsarSinkConfig sinkConfig =
        JsonUtils.fromJson(json, new TypeReference<PulsarSinkConfig>() {}).orElse(null);
    PulsarSink pulsarSink = new PulsarSink(sinkConfig);
    pulsarSink.init();
    int i = 0;
    while (i < 1000) {
      RowRecord record =
          SourceRowRecord.builder()
              .fieldNames(new String[] {"id", "name", "password"})
              .fieldTypes(
                  new DataWorkflowFieldType[] {
                    BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
                  })
              .values(new Object[] {i, "shawn" + i, "123456"})
              .build();
      pulsarSink.write(record);
      i++;
    }

    pulsarSink.commit();
    TimeUnit.SECONDS.sleep(2);
    pulsarSink.dispose();
  }

  @Test
  public void testPulsarSink2() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/pulsar/pulsar-sink-2.json")),
            StandardCharsets.UTF_8);
    PulsarSinkConfig sinkConfig =
        JsonUtils.fromJson(json, new TypeReference<PulsarSinkConfig>() {}).orElse(null);
    PulsarSink pulsarSink = new PulsarSink(sinkConfig);
    pulsarSink.init();
    TransformRowRecord record =
        new TransformRowRecord(
            new String[] {"id", "name", "password"},
            new DataWorkflowFieldType[] {
              BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
            });
    int i = 0;
    while (i < 1000) {
      record.addRecord(new Object[] {i, "shawn" + i, "123456"});
      i++;
    }
    pulsarSink.write(record);

    pulsarSink.commit();
    TimeUnit.SECONDS.sleep(2);
    pulsarSink.dispose();
  }
}
