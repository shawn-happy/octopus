package io.github.octopus.datos.centro.sql.model.dialect.doris;

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
}
