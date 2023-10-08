package com.octopus.actus.connector.jdbc.model.dialect.doris;

import com.octopus.actus.connector.jdbc.model.ColumnKey;
import java.util.Arrays;

public enum DorisColumnKey implements ColumnKey {
  DUPLICATE_KEY("DUP"),
  AGGREGATE_KEY("AGG"),
  UNIQUE_KEY("UNI"),
  ;

  private final String key;

  DorisColumnKey(String key) {
    this.key = key;
  }

  @Override
  public String getKey() {
    return key;
  }

  public static DorisColumnKey of(String columnKey) {
    return Arrays.stream(values())
        .filter(key -> key.getKey().equalsIgnoreCase(columnKey))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the column key [%s] is not supported with doris", columnKey)));
  }
}
