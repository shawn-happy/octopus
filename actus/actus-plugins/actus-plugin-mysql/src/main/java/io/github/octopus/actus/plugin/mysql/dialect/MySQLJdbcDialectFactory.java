package io.github.octopus.actus.plugin.mysql.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.core.model.FieldIdeEnum;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialectFactory;
import lombok.Getter;

@Getter
public class MySQLJdbcDialectFactory implements JdbcDialectFactory {

  private final String dialectName = DatabaseIdentifier.MYSQL;

  @Override
  public JdbcDialect create() {
    return new MySQLJdbcDialect();
  }

  @Override
  public JdbcDialect create(FieldIdeEnum fieldIde) {
    return new MySQLJdbcDialect(fieldIde);
  }
}
