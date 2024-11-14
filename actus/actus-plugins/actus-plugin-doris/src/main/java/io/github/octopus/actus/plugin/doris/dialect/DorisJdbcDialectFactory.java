package io.github.octopus.actus.plugin.doris.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.core.model.FieldIdeEnum;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialectFactory;
import lombok.Getter;

@Getter
public class DorisJdbcDialectFactory implements JdbcDialectFactory {

  private final String dialectName = DatabaseIdentifier.DORIS;

  @Override
  public JdbcDialect create() {
    return new DorisJdbcDialect();
  }

  @Override
  public JdbcDialect create(FieldIdeEnum fieldIde) {
    return new DorisJdbcDialect(fieldIde);
  }
}
