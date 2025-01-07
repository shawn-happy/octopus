package io.github.octopus.actus.plugin.oracle.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.core.model.FieldIdeEnum;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialectFactory;
import lombok.Getter;

@Getter
public class OracleJdbcDialectFactory implements JdbcDialectFactory {

  private final String dialectName = DatabaseIdentifier.ORACLE;

  @Override
  public JdbcDialect create() {
    return new OracleJdbcDialect();
  }

  @Override
  public JdbcDialect create(FieldIdeEnum fieldIde) {
    return new OracleJdbcDialect(fieldIde);
  }
}
