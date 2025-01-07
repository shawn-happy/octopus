package io.github.octopus.actus.core.model.schema;

import java.util.Arrays;

public enum IntervalType {
  YEAR,
  MONTH,
  WEEK,
  DAY,
  ;

  public static IntervalType of(String type) {
    return Arrays.stream(values())
        .filter(intervalType -> intervalType.name().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("The intervalType [%s] is not supported", type)));
  }
}
