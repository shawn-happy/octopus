package io.github.octopus.sql.executor.plugin.doris.model;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public enum AggregateType {
  DUPLICATE_KEY("DUPLICATE KEY"),
  AGGREGATE_KEY("AGGREGATE KEY"),
  UNIQUE_KEY("UNIQUE KEY"),
  ;

  private final String key;

  AggregateType(String key) {
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  private static final Map<String, AggregateType> TYPE_MAP =
      ImmutableMap.of("DUPLICATE", DUPLICATE_KEY, "AGGREGATE", AGGREGATE_KEY, "UNIQUE", UNIQUE_KEY);

  public static AggregateType of(String type) {
    return Optional.ofNullable(
            Arrays.stream(values())
                .filter(aggregateType -> aggregateType.getKey().equalsIgnoreCase(type))
                .findFirst()
                .orElse(TYPE_MAP.get(type)))
        .orElseThrow(
            () -> new IllegalStateException(String.format("no such aggregate type [%s]", type)));
  }
}
