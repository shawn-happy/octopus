package io.github.octopus.sql.executor.plugin.api.dialect;

import io.github.octopus.sql.executor.core.model.FieldIdeEnum;

public interface JdbcDialectFactory {
  /**
   * Get the name of jdbc dialect.
   *
   * @return the dialect name.
   */
  String getDialectName();

  /** @return Creates a new instance of the {@link JdbcDialect}. */
  JdbcDialect create();

  /**
   * Create a {@link JdbcDialect} instance based on the driver type and compatible mode.
   *
   * @return a new instance of {@link JdbcDialect}
   */
  default JdbcDialect create(FieldIdeEnum fieldIde) {
    return create();
  }
}
