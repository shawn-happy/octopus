package io.github.octopus.sql.executor.plugin.doris.model;

import io.github.octopus.sql.executor.core.model.schema.TableEngine;
import java.util.Arrays;

public enum DorisTableEngine implements TableEngine {
  OLAP("Doris"),
  VIEW("VIEW"),
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
