package com.octopus.actus.connector.jdbc;

import java.util.Arrays;

public enum DbType {
  oracle,
  mysql,
  doris,
  ;

  public static DbType of(String name) {
    return Arrays.stream(values())
        .filter(type -> type.name().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("unsupported db type " + name));
  }
}
