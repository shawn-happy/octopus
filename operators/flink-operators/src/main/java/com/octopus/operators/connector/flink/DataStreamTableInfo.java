package com.octopus.operators.connector.flink;

import lombok.Getter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

@Getter
public class DataStreamTableInfo {

  private DataStream<Row> dataStream;
  private String tableName;

  private DataStreamTableInfo(String tableName, DataStream<Row> dataStream) {
    this.tableName = tableName;
    this.dataStream = dataStream;
  }

  public static DataStreamTableInfo of(String tableName, DataStream<Row> dataStream) {
    return new DataStreamTableInfo(tableName, dataStream);
  }
}
