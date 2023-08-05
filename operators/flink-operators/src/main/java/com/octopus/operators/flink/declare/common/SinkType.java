package com.octopus.operators.flink.declare.common;

import java.util.List;

public enum SinkType {
  parquet,
  csv,
  json,
  jdbc,
  iceberg,
  hive,
  console,
  ;

  public static List<SinkType> getFileFormat() {
    return List.of(csv, parquet, json);
  }

  public static boolean isFileFormat(String fileFormat) {
    return getFileFormat().stream().anyMatch(type -> type.name().equalsIgnoreCase(fileFormat));
  }
}
