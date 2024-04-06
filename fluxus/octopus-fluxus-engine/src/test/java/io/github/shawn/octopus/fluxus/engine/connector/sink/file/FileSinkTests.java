package io.github.shawn.octopus.fluxus.engine.connector.sink.file;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class FileSinkTests {
  @Test
  public void testJsonLocalFileSink() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/file/local-json-sink.json")),
            StandardCharsets.UTF_8);
    FileSinkConfig sinkConfig =
        JsonUtils.fromJson(json, new TypeReference<FileSinkConfig>() {}).get();
    FileSink fileSink = new FileSink(sinkConfig);
    fileSink.init();

    fileSink.write(
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "age"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.INT
                })
            .values(new Object[] {1, "shawn", 28})
            .build());
    fileSink.write(
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "age"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.INT
                })
            .values(new Object[] {2, "shawn2", 28})
            .build());
    fileSink.dispose();
  }

  @Test
  public void testCSVLocalFileSink() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/file/local-csv-sink.json")),
            StandardCharsets.UTF_8);
    FileSinkConfig sinkConfig =
        JsonUtils.fromJson(json, new TypeReference<FileSinkConfig>() {}).get();
    FileSink fileSink = new FileSink(sinkConfig);
    fileSink.init();

    fileSink.write(
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "age"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.INT
                })
            .values(new Object[] {1, "shawn", 28})
            .build());
    fileSink.write(
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "age"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.INT
                })
            .values(new Object[] {2, "shawn2", 28})
            .build());
    fileSink.dispose();
  }
}
