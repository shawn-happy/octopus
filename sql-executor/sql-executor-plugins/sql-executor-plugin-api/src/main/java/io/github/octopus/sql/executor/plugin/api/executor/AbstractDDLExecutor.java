package io.github.octopus.sql.executor.plugin.api.executor;

import javax.sql.DataSource;

public abstract class AbstractDDLExecutor implements DDLExecutor {

  protected static final String BLANK_COMMENT = "";

  private final String name;

  protected AbstractDDLExecutor(String name, DataSource dataSource) {
    this.name = name;
  }
}
