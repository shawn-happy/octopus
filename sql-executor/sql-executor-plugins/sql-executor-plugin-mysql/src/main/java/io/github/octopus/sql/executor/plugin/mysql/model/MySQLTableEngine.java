package io.github.octopus.sql.executor.plugin.mysql.model;

import io.github.octopus.sql.executor.core.model.schema.TableEngine;
import java.util.Arrays;

public enum MySQLTableEngine implements TableEngine {
  INNODB("InnoDB"),
  MYISAM("MyISAM"),
  ;

  private final String engine;

  MySQLTableEngine(String engine) {
    this.engine = engine;
  }

  @Override
  public String getEngine() {
    return engine;
  }

  public static MySQLTableEngine of(String type) {
    return Arrays.stream(values())
        .filter(dorisEngine -> dorisEngine.getEngine().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the engine [%s] is unsupported with mysql", type)));
  }
}
