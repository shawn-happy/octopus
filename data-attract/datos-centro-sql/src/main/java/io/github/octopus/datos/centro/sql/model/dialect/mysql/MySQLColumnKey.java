package io.github.octopus.datos.centro.sql.model.dialect.mysql;

import io.github.octopus.datos.centro.sql.model.ColumnKey;
import java.util.Arrays;

public enum MySQLColumnKey implements ColumnKey {
  PRIMARY_KEY("PRI"),
  NORMAL_KEY("MUL"),
  UNIQUE_KEY("UNI"),
  ;

  private final String key;

  MySQLColumnKey(String key) {
    this.key = key;
  }

  @Override
  public String getKey() {
    return key;
  }

  public static MySQLColumnKey of(String columnKey) {
    return Arrays.stream(values())
        .filter(key -> key.getKey().equalsIgnoreCase(columnKey))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the column key [%s] is not supported with mysql", columnKey)));
  }
}
