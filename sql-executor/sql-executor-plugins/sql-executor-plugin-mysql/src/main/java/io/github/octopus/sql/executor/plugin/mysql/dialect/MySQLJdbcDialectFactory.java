package io.github.octopus.sql.executor.plugin.mysql.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialectFactory;
import lombok.Getter;

@Getter
public class MySQLJdbcDialectFactory implements JdbcDialectFactory {

  private final String dialectName = DatabaseIdentifier.MYSQL;

  @Override
  public JdbcDialect create() {
    return new MySQLJdbcDialect();
  }

  @Override
  public JdbcDialect create(String fieldId) {
    return new MySQLJdbcDialect(fieldId);
  }
}
