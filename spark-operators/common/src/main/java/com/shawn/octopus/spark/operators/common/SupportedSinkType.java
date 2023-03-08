package com.shawn.octopus.spark.operators.common;

import java.util.List;

public enum SupportedSinkType {
  parquet,
  csv,
  json,
  jdbc,
  iceberg,
  hive,
  ;

  public static List<SupportedSinkType> getFileFormat() {
    return List.of(csv, parquet, json);
  }

  public static boolean isFileFormat(String fileFormat) {
    return getFileFormat().stream().anyMatch(type -> type.name().equalsIgnoreCase(fileFormat));
  }
}
