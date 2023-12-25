package com.octopus.operators.flink.runtime;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class FlinkJobContext {

  @Getter private final StreamExecutionEnvironment executionEnvironment;
  private final Map<String, DataStream<Row>> dataStreamMap = new HashMap<>();

  public FlinkJobContext(StreamExecutionEnvironment executionEnvironment) {
    this.executionEnvironment = executionEnvironment;
  }

  public void setDataStream(String tableName, DataStream<Row> df) {
    this.dataStreamMap.computeIfAbsent(tableName, e -> df);
  }

  public DataStream<Row> getDataStream(String tableName) {
    return this.dataStreamMap.get(tableName);
  }
}
