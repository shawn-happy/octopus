package com.octopus.spark.operators.runtime.executor;

import com.octopus.spark.operators.exception.SparkRuntimeException;
import java.util.Arrays;

public enum ExecutorType {
  ETL,
  DATA_QUALITY,
  REPORT,
  ;

  public static ExecutorType of(String type) {
    return Arrays.stream(values())
        .filter(executorType -> executorType.name().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(() -> new SparkRuntimeException("unsupported executor type: " + type));
  }
}
