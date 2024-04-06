package io.github.shawn.octopus.fluxus.engine.connector.sink.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.table.TransformRowRecord;
import io.github.shawn.octopus.fluxus.engine.model.type.BasicFieldType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

public class JdbcSinkTests {

  @Test
  public void testInsert() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/jdbc/jdbc-sink-insert.json")),
            StandardCharsets.UTF_8);
    JdbcSinkConfig jdbcSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<JdbcSinkConfig>() {}).orElse(null);
    assertNotNull(jdbcSinkConfig);
    JdbcSink jdbcSink = new JdbcSink(jdbcSinkConfig);
    jdbcSink.init();
    jdbcSink.begin();
    RowRecord record =
        SourceRowRecord.builder()
            .fieldNames(new String[] {"id", "name", "password"})
            .fieldTypes(
                new DataWorkflowFieldType[] {
                  BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
                })
            .values(new Object[] {1, "shawn", "123456"})
            .build();
    jdbcSink.write(record);
    //    TimeUnit.SECONDS.sleep(10);
    jdbcSink.commit();
    jdbcSink.dispose();
  }

  @Test
  public void testInsertEqualsBatchSize() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/jdbc/jdbc-sink-insert.json")),
            StandardCharsets.UTF_8);
    JdbcSinkConfig jdbcSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<JdbcSinkConfig>() {}).orElse(null);
    assertNotNull(jdbcSinkConfig);
    JdbcSink jdbcSink = new JdbcSink(jdbcSinkConfig);
    jdbcSink.init();
    jdbcSink.begin();
    TransformRowRecord record =
        new TransformRowRecord(
            new String[] {"name", "password"},
            new DataWorkflowFieldType[] {BasicFieldType.STRING, BasicFieldType.STRING});
    record.addRecords(fake(1000, 2));
    jdbcSink.write(record);
    jdbcSink.commit();
    jdbcSink.dispose();
  }

  @Test
  public void testInsertGTBatchSize() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/jdbc/jdbc-sink-insert.json")),
            StandardCharsets.UTF_8);
    JdbcSinkConfig jdbcSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<JdbcSinkConfig>() {}).orElse(null);
    assertNotNull(jdbcSinkConfig);
    JdbcSink jdbcSink = new JdbcSink(jdbcSinkConfig);
    jdbcSink.init();
    jdbcSink.begin();
    TransformRowRecord record =
        new TransformRowRecord(
            new String[] {"name", "password"},
            new DataWorkflowFieldType[] {BasicFieldType.STRING, BasicFieldType.STRING});
    record.addRecords(fake(1200, 2));
    jdbcSink.write(record);
    jdbcSink.commit();
    jdbcSink.dispose();
  }

  @Test
  public void testUpdateGTBatchSize() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/jdbc/jdbc-sink-update.json")),
            StandardCharsets.UTF_8);
    JdbcSinkConfig jdbcSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<JdbcSinkConfig>() {}).orElse(null);
    assertNotNull(jdbcSinkConfig);
    JdbcSink jdbcSink = new JdbcSink(jdbcSinkConfig);
    jdbcSink.init();
    jdbcSink.begin();
    TransformRowRecord record =
        new TransformRowRecord(
            new String[] {"id", "name", "password"},
            new DataWorkflowFieldType[] {
              BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
            });
    record.addRecords(fake(1200));
    jdbcSink.write(record);
    jdbcSink.commit();
    jdbcSink.dispose();
  }

  @Test
  public void testUpsertGTBatchSize() throws Exception {
    String json =
        Resources.toString(
            Objects.requireNonNull(
                Thread.currentThread()
                    .getContextClassLoader()
                    .getResource("example/sink/jdbc/jdbc-sink-upsert.json")),
            StandardCharsets.UTF_8);
    JdbcSinkConfig jdbcSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<JdbcSinkConfig>() {}).orElse(null);
    assertNotNull(jdbcSinkConfig);
    JdbcSink jdbcSink = new JdbcSink(jdbcSinkConfig);
    jdbcSink.init();
    jdbcSink.begin();
    TransformRowRecord record =
        new TransformRowRecord(
            new String[] {"id", "name", "password"},
            new DataWorkflowFieldType[] {
              BasicFieldType.INT, BasicFieldType.STRING, BasicFieldType.STRING
            });
    record.addRecords(fake(1500));
    jdbcSink.write(record);
    jdbcSink.commit();
    jdbcSink.dispose();
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

  private static List<Object[]> fake(int size, int colSize) {
    List<Object[]> rowRecords = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      Object[] value = new Object[colSize];
      for (int j = 0; j < colSize; j++) {
        value[j] = RandomStringUtils.randomAlphabetic(6);
      }
      rowRecords.add(value);
    }
    return rowRecords;
  }
}
