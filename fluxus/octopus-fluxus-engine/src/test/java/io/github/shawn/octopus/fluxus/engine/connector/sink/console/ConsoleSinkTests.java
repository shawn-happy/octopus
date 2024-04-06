package io.github.shawn.octopus.fluxus.engine.connector.sink.console;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

public class ConsoleSinkTests {

  @Test
  public void testConsole() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/console/console.json")),
            StandardCharsets.UTF_8);
    ConsoleSinkConfig consoleSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<ConsoleSinkConfig>() {}).orElse(null);
    assertNotNull(consoleSinkConfig);
    ConsoleSink sink = new ConsoleSink(consoleSinkConfig);
    TransformRowRecord rowRecord =
        new TransformRowRecord(
            new String[] {"id", "name", "password"},
            new DataWorkflowFieldType[] {
              BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
            });
    rowRecord.addRecords(fake(1200));
    sink.init();
    sink.begin();
    sink.write(rowRecord);
    sink.commit();
    sink.dispose();
  }

  private static List<Object[]> fake(int size) {
    List<Object[]> rowRecords = new ArrayList<>(size);
    int inx = 0;
    for (int i = 0; i < size; i++) {
      Object[] value = new Object[3];
      value[0] = ++inx;
      for (int j = 1; j < 3; j++) {
        value[j] = RandomStringUtils.randomAlphabetic(6);
      }
      rowRecords.add(value);
    }
    return rowRecords;
  }
}
