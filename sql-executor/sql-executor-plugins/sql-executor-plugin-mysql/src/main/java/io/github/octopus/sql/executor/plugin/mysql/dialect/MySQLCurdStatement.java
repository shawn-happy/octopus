package io.github.octopus.sql.executor.plugin.mysql.dialect;

import io.github.octopus.sql.executor.core.StringPool;
import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import io.github.octopus.sql.executor.plugin.api.dialect.CurdStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.DialectRegistry;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MySQLCurdStatement implements CurdStatement {

  private static final MySQLCurdStatement CURD_STATEMENT = new MySQLCurdStatement();

  private MySQLCurdStatement() {}

  public static CurdStatement getCurdStatement() {
    return CURD_STATEMENT;
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.MYSQL);
  }

  @Override
  public Optional<String> getUpsertSql(UpsertStatement upsertStatement) {
    List<String> columns =
        upsertStatement
            .getColumns()
            .stream()
            .map(ColumnDefinition::getColumn)
            .collect(Collectors.toList());
    String updateClause =
        columns
            .stream()
            .map(
                fieldName ->
                    quoteIdentifier(fieldName) + "=VALUES(" + quoteIdentifier(fieldName) + ")")
            .collect(Collectors.joining(", "));
    InsertStatement insertStatement =
        InsertStatement.builder()
            .tablePath(upsertStatement.getTablePath())
            .columns(columns)
            .build();
    String upsertSQL = getInsertSql(insertStatement) + " ON DUPLICATE KEY UPDATE " + updateClause;
    return Optional.of(upsertSQL);
  }

  @Override
  public String getTruncateTableSql(TablePath tablePath) {
    return String.format(
        "TRUNCATE TABLE `%s`", tablePath.getFullNameWithQuoted(getJdbcDialect().getFieldIde()));
  }

  @Override
  public String buildPageSql(String originalSql, long offset, long limit) {
    StringBuilder sql = new StringBuilder(originalSql).append(" LIMIT ").append(offset);
    if (offset != 0L) {
      sql.append(StringPool.COMMA).append(limit);
    }
    return sql.toString();
  }
}
