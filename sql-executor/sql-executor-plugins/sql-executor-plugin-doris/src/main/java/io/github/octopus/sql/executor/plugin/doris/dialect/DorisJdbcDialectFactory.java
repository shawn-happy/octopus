package io.github.octopus.sql.executor.plugin.doris.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialectFactory;
import lombok.Getter;

@Getter
public class DorisJdbcDialectFactory implements JdbcDialectFactory {

  private final String dialectName = DatabaseIdentifier.DORIS;

  @Override
  public JdbcDialect create() {
    return new DorisJdbcDialect();
  }

  @Override
  public JdbcDialect create(String fieldId) {
    return new DorisJdbcDialect(fieldId);
  }
}
