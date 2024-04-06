package io.github.shawn.octopus.fluxus.engine.connector.sink.doris;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import io.github.shawn.octopus.fluxus.engine.pipeline.config.JobConfig;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContext;
import io.github.shawn.octopus.fluxus.engine.pipeline.context.JobContextManagement;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DorisSinkTests {

  @Test
  public void testStreamLoadByCSV() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/doris/doris-csv-format.json")),
            StandardCharsets.UTF_8);
    JobConfig mock = Mockito.mock(JobConfig.class);
    JobContext context = Mockito.mock(JobContext.class);
    JobContextManagement.setJob(context);
    Mockito.when(context.getJobConfig()).thenReturn(mock);
    Mockito.when(mock.getJobId()).thenReturn(IdGenerator.uuid());
    DorisSinkConfig dorisSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<DorisSinkConfig>() {}).orElse(null);
    assertNotNull(dorisSinkConfig);
    DorisSink dorisSink = new DorisSink(dorisSinkConfig);
    dorisSink.init();
    dorisSink.begin();
    RowRecord record =
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "password"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
                })
            .values(new Object[] {1, "shawn", "123456"})
            .build();
    dorisSink.write(record);
    dorisSink.commit();
    dorisSink.dispose();
  }

  @Test
  public void testStreamLoadByJSON() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/doris/doris-json-format.json")),
            StandardCharsets.UTF_8);
    JobConfig mock = Mockito.mock(JobConfig.class);
    JobContext context = Mockito.mock(JobContext.class);
    JobContextManagement.setJob(context);
    Mockito.when(context.getJobConfig()).thenReturn(mock);
    Mockito.when(mock.getJobId()).thenReturn(IdGenerator.uuid());
    DorisSinkConfig dorisSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<DorisSinkConfig>() {}).orElse(null);
    assertNotNull(dorisSinkConfig);
    DorisSink dorisSink = new DorisSink(dorisSinkConfig);
    dorisSink.init();
    dorisSink.begin();
    RowRecord record =
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "password"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
                })
            .values(new Object[] {2, "shawn", "123456"})
            .build();
    dorisSink.write(record);
    dorisSink.commit();
    dorisSink.dispose();
  }
}
