package com.octopus.operators.connector.flink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkJobContext {

  @Getter private final StreamExecutionEnvironment executionEnvironment;
  @Getter private final StreamTableEnvironment tableEnvironment;
  private final Map<String, DataStream<Row>> dataStreamMap = new HashMap<>();

  public FlinkJobContext(FlinkRuntimeEnvironment flinkRuntimeEnvironment) {
    this.executionEnvironment = flinkRuntimeEnvironment.getStreamExecutionEnvironment();
    this.tableEnvironment = flinkRuntimeEnvironment.getStreamTableEnvironment();
  }

  public void registerTable(String table, DataStream<Row> ds) {
    boolean exists = Arrays.asList(tableEnvironment.listTables()).contains(table);
    if (!exists) {
      tableEnvironment.createTemporaryView(table, ds);
    }
    dataStreamMap.putIfAbsent(table, ds);
  }

  public DataStream<Row> getDataStream(String tableName) {
    return this.dataStreamMap.get(tableName);
  }

  public boolean tableExists(List<String> tables) {
    if (CollectionUtils.isEmpty(tables)) {
      return false;
    }
    if (MapUtils.isEmpty(dataStreamMap)) {
      return false;
    }
    for (String table : tables) {
      if (!dataStreamMap.containsKey(table)) {
        return false;
      }
    }
    return true;
  }
}
