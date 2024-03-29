package com.octopus.actus.connector.jdbc.model.dialect.doris;

import com.octopus.actus.connector.jdbc.model.TableEngine;
import java.util.Arrays;

public enum DorisTableEngine implements TableEngine {
  OLAP("Doris"),
  ;

  private final String engine;

  DorisTableEngine(String engine) {
    this.engine = engine;
  }

  @Override
  public String getEngine() {
    return engine;
  }

  public static DorisTableEngine of(String type) {
    return Arrays.stream(values())
        .filter(dorisEngine -> dorisEngine.getEngine().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the engine [%s] is unsupported with doris", type)));
  }
}
