package com.octopus.spark.operators.declare.common;

import java.util.Arrays;

public enum FieldType {
  Boolean,
  Integer,
  Long,
  Float,
  Double,
  String,
  Date,
  Timestamp,
  ;

  public static FieldType of(String type) {
    return Arrays.stream(values())
        .filter(fieldType -> fieldType.name().equals(type))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    java.lang.String.format("The data type [%s] is not supported", type)));
  }
}
