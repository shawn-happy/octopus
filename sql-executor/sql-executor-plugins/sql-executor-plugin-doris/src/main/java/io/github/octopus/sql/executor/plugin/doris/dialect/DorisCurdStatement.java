package io.github.octopus.sql.executor.plugin.doris.dialect;

import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.CurdStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import java.util.Optional;

public class DorisCurdStatement implements CurdStatement {

  private static final DorisCurdStatement CURD_STATEMENT = new DorisCurdStatement();

  private DorisCurdStatement() {}

  public static CurdStatement getCurdStatement() {
    return CURD_STATEMENT;
  }

  @Override
  public Optional<String> getUpsertSql(UpsertStatement upsertStatement) {
    return Optional.empty();
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return null;
  }
}
