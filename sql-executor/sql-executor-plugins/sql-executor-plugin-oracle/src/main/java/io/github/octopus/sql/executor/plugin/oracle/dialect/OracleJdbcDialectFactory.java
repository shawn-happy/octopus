package io.github.octopus.sql.executor.plugin.oracle.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialectFactory;
import lombok.Getter;

@Getter
public class OracleJdbcDialectFactory implements JdbcDialectFactory {

  private final String dialectName = DatabaseIdentifier.ORACLE;

  @Override
  public JdbcDialect create() {
    return new OracleJdbcDialect();
  }

  @Override
  public JdbcDialect create(String fieldId) {
    return new OracleJdbcDialect(fieldId);
  }
}
