package io.github.octopus.sql.executor.plugin.sqlserver.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialectFactory;

public class SqlServerJdbcDialectFactory implements JdbcDialectFactory {

  @Override
  public String getDialectName() {
    return DatabaseIdentifier.SQLSERVER;
  }

  @Override
  public JdbcDialect create() {
    return new SqlServerJdbcDialect();
  }

  @Override
  public JdbcDialect create(FieldIdeEnum fieldIde) {
    return new SqlServerJdbcDialect(fieldIde);
  }
}
