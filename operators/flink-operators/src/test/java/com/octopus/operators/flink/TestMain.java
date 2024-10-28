package com.octopus.operators.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class TestMain {

  public static final ResolvedSchema SCHEMA =
      ResolvedSchema.of(
          Column.physical("id", DataTypes.INT()),
          Column.physical("b", DataTypes.INT()),
          Column.physical("c", DataTypes.INT()));

  public static final DataType PHYSICAL_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

  public static final RowType PHYSICAL_TYPE = (RowType) PHYSICAL_DATA_TYPE.getLogicalType();

  public static final String MESSAGE =
      "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"a\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"b\"}],\"optional\":true,\"name\":\"test_shawn.test_shawn.test.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"a\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"b\"}],\"optional\":true,\"name\":\"test_shawn.test_shawn.test.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"test_shawn.test_shawn.test.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":1,\"a\":1,\"b\":1},\"source\":{\"version\":\"1.9.8.Final\",\"connector\":\"mysql\",\"name\":\"test_shawn\",\"ts_ms\":1716372092000,\"snapshot\":\"true\",\"db\":\"test_shawn\",\"sequence\":null,\"table\":\"test\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000216\",\"pos\":437173598,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"r\",\"ts_ms\":1716372092897,\"transaction\":null}}";

  @Test
  public void testDeserialization() throws Exception {
    final DebeziumJsonDeserializationSchema debeziumJsonDeserializationSchema =
        new DebeziumJsonDeserializationSchema(
            PHYSICAL_DATA_TYPE,
            Collections.emptyList(),
            InternalTypeInfo.of(PHYSICAL_TYPE),
            false,
            false,
            TimestampFormat.ISO_8601);
    open(debeziumJsonDeserializationSchema);
    debeziumJsonDeserializationSchema.deserialize(
        MESSAGE.getBytes(StandardCharsets.UTF_8), new SimpleCollector());
  }

  @Test
  public void test() {
    final String tobeEncode = "root" + ":" + "root";
    byte[] encoded = Base64.getEncoder().encode(tobeEncode.getBytes(StandardCharsets.UTF_8));
    String s = "Basic " + new String(encoded);
    System.out.println(s);
  }

  private static final String CREATE_KAFKA_TABLE =
      "CREATE TABLE test_copy\n"
          + "(\n"
          + "    id int,\n"
          + "    a  int,\n"
          + "    b  int\n"
          + ")\n"
          + "WITH ( 'connector' = 'kafka',\n"
          + "    'topic' = 'test_shawn.test_shawn.test',\n"
          + "    'properties.bootstrap.servers' = '192.168.5.51:9092',\n"
          + "    'properties.group.id' = 'test_group_1',\n"
          + "    'scan.startup.mode' = 'earliest-offset',\n"
          + "    'value.format' = 'debezium-json',\n"
          + "    'value.debezium-json.schema-include' = 'true');";

  private static final String CREATE_DORIS_TABLE =
      "CREATE TABLE test2\n"
          + "(\n"
          + "    id int,\n"
          + "    a int,\n"
          + "    b int\n"
          + ")\n"
          + "WITH ( 'connector' = 'doris',\n"
          + "    'fenodes' = '192.168.5.22:8030',\n"
          + "    'table.identifier' = 'test_shawn.test2',\n"
          + "    'username' = 'root',\n"
          + "    'sink.label-prefix' = 'test2-2',\n"
          + "    'password' = 'root');";

  @Test
  public void testFlinkSQL() throws Exception {
    EnvironmentSettings environmentSettings =
        EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment executionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();
    TableEnvironment tableEnvironment =
        StreamTableEnvironment.create(executionEnvironment, environmentSettings);
    tableEnvironment.executeSql(CREATE_KAFKA_TABLE);
    tableEnvironment.executeSql(CREATE_DORIS_TABLE);
    tableEnvironment.executeSql("insert into test2 (id, a, b) select id,a,b from test_copy");
    tableEnvironment.sqlQuery("select * from test2").execute().print();
  }

  @Test
  public void test2(){
    System.out.println(System.getProperty("user.dir"));
  }

  private static class SimpleCollector implements Collector<RowData> {

    private List<RowData> list = new ArrayList<>();

    @Override
    public void collect(RowData record) {
      list.add(record);
    }

    @Override
    public void close() {
      // do nothing
    }
  }

  public static void open(DeserializationSchema<?> schema) {
    try {
      schema.open(new DummyInitializationContext());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class DummyInitializationContext
      implements SerializationSchema.InitializationContext,
          DeserializationSchema.InitializationContext {

    @Override
    public MetricGroup getMetricGroup() {
      return new UnregisteredMetricsGroup();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
      return SimpleUserCodeClassLoader.create(DummyInitializationContext.class.getClassLoader());
    }
  }
}
