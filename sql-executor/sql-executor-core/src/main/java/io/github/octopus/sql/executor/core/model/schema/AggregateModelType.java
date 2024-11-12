package io.github.octopus.sql.executor.core.model.schema;

import lombok.Getter;

@Getter
public enum AggregateModelType {
  DUPLICATE_KEY("DUPLICATE KEY"),
  UNIQUE_KEY("UNIQUE KEY"),
  AGGREGATE_KEY("AGGREGATE KEY"),
  ;

  private final String type;

  AggregateModelType(final String type) {
    this.type = type;
  }
}
