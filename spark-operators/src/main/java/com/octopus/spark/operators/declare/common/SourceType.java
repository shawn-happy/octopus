package com.octopus.spark.operators.declare.common;

import com.octopus.spark.operators.exception.SparkRuntimeException;
import java.util.Arrays;
import java.util.List;

public enum SourceType {
  csv,
  parquet,
  json,
  jdbc,
  iceberg,
  hive,
  ;

  public static List<SourceType> getFileFormat() {
    return List.of(csv, parquet, json);
  }

  public static boolean isFileFormat(String fileFormat) {
    return getFileFormat().stream().anyMatch(type -> type.name().equalsIgnoreCase(fileFormat));
  }

  public static boolean isFileFormat(SourceType fileFormat) {
    return getFileFormat().stream().anyMatch(type -> type.equals(fileFormat));
  }

  public static void validate(SourceType sourceType) {
    Arrays.stream(values())
        .filter(type -> type == sourceType)
        .findFirst()
        .orElseThrow(() -> new SparkRuntimeException("unsupported source type: " + sourceType));
  }
}
