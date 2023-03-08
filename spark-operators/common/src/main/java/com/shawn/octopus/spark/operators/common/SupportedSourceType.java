package com.shawn.octopus.spark.operators.common;

import java.util.List;

public enum SupportedSourceType {
  csv,
  parquet,
  json,
  jdbc,
  iceberg,
  hive,
  ;

  public static List<SupportedSourceType> getFileFormat() {
    return List.of(csv, parquet, json);
  }

  public static boolean isFileFormat(String fileFormat) {
    return getFileFormat().stream().anyMatch(type -> type.name().equalsIgnoreCase(fileFormat));
  }

  public static boolean isFileFormat(SupportedSourceType fileFormat) {
    return getFileFormat().stream().anyMatch(type -> type.equals(fileFormat));
  }
}
