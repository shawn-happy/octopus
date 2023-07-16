package com.octopus.operators.spark.runtime;

import com.octopus.operators.spark.exception.SparkRuntimeException;
import java.util.Arrays;

public enum SparkRuntimeMode {
  LOCAL,
  YARN,
  K8S,
  ;

  public static SparkRuntimeMode of(String type) {
    return Arrays.stream(values())
        .filter(mode -> mode.name().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(() -> new SparkRuntimeException("unsupported spark runtime mode: " + type));
  }
}
