package com.octopus.operators.flink.runtime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataStreamTableInfo {

  private DataStream<Row> dataStream;
  private String tableName;
}
