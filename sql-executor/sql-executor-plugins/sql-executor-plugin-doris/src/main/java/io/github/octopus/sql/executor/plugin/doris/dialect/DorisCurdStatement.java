package io.github.octopus.sql.executor.plugin.doris.dialect;

import io.github.octopus.sql.executor.core.StringPool;
import io.github.octopus.sql.executor.core.exception.SqlException;
import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import io.github.octopus.sql.executor.plugin.api.dialect.CurdStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.DialectRegistry;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DorisCurdStatement implements CurdStatement {

  private static final DorisCurdStatement CURD_STATEMENT = new DorisCurdStatement();

  private DorisCurdStatement() {}

  public static CurdStatement getCurdStatement() {
    return CURD_STATEMENT;
  }

  @Override
  public Optional<String> getInsertBatchSql(InsertStatement insertStatement) {
    TablePath tablePath = insertStatement.getTablePath();
    List<String> fieldNames = insertStatement.getColumns();
    String columns =
            fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
    List<Object[]> values = insertStatement.getValues();
    if (CollectionUtils.isEmpty(values)) {
      throw new SqlException("values cannot be empty");
    }
    StringBuilder builder = new StringBuilder();
    String placeholders =
            fieldNames.stream().map(fieldName -> "?").collect(Collectors.joining(", "));
    for (int i = 0; i < values.size(); i++) {
      builder.append("(");
      builder.append(placeholders);
      builder.append(")");
      if (i != values.size() - 1) {
        builder.append(", ");
      }
    }

    return Optional.of(
            String.format(
                    "INSERT INTO %s (%s) VALUES %s", tableIdentifier(tablePath), columns, builder));
  }

  @Override
  public Optional<String> getUpsertSql(UpsertStatement upsertStatement) {
    List<String> columns =
        upsertStatement.getColumns().stream()
            .map(ColumnDefinition::getColumn)
            .collect(Collectors.toList());
    String updateClause =
        columns.stream()
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
    return String.format("TRUNCATE TABLE %s", tableIdentifier(tablePath));
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.DORIS);
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
