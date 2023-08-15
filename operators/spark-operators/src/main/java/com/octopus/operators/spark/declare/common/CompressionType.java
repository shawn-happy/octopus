package com.octopus.operators.spark.declare.common;

import java.util.Arrays;

public enum CompressionType {
  snappy,
  gzip,
  lz4,
  ;

  public static CompressionType of(String compressionType) {
    return Arrays.stream(values())
        .filter(type -> type.name().equalsIgnoreCase(compressionType))
        .findFirst()
        .orElseThrow(
            () -> new RuntimeException("unsupported compression type: " + compressionType));
  }
}
