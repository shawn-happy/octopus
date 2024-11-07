package io.github.octopus.sql.executor.plugin.oracle.dialect;

import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.CurdStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import java.util.Optional;

public class OracleCurdStatement implements CurdStatement {

  private static final OracleCurdStatement CURD_STATEMENT = new OracleCurdStatement();

  private OracleCurdStatement() {}

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

  @Override
  public String buildPageSql(String originalSql, long offset, long limit) {
    limit = (offset >= 1) ? (offset + limit) : limit;
    return "SELECT * FROM ( SELECT TMP.*, ROWNUM ROW_ID FROM ( "
        + originalSql
        + " ) TMP WHERE ROWNUM <="
        + limit
        + ") WHERE ROW_ID > "
        + offset;
  }
}
