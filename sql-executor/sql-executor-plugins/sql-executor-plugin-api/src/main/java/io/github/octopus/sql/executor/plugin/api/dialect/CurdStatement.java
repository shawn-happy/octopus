package io.github.octopus.sql.executor.plugin.api.dialect;

import static java.lang.String.format;

import io.github.octopus.sql.executor.core.exception.SqlException;
import io.github.octopus.sql.executor.core.model.curd.DeleteStatement;
import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.RowExistsStatement;
import io.github.octopus.sql.executor.core.model.curd.UpdateStatement;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.core.model.expression.Expression;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public interface CurdStatement extends SqlStatement {

  default String getInsertSql(InsertStatement insertStatement) {
    TablePath tablePath = insertStatement.getTablePath();
    List<String> fieldNames = insertStatement.getColumns();
    String columns =
        fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
    String placeholders =
        fieldNames.stream().map(fieldName -> ":" + fieldName).collect(Collectors.joining(", "));
    return String.format(
        "INSERT INTO %s (%s) VALUES (%s)", tableIdentifier(tablePath), columns, placeholders);
  }

  default String getInsertBatchSql(InsertStatement insertStatement) {
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

    return String.format(
        "INSERT INTO %s (%s) VALUES %s", tableIdentifier(tablePath), columns, builder);
  }

  default String getUpdateSql(UpdateStatement updateStatement) {
    LinkedHashMap<String, Object> updateParams = updateStatement.getUpdateParams();
    Set<String> fieldNames = updateParams.keySet();
    String setClause =
        fieldNames
            .stream()
            .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
            .collect(Collectors.joining(", "));
    TablePath tablePath = updateStatement.getTablePath();
    Expression expression = updateStatement.getExpression();
    if (Objects.isNull(expression)) {
      return String.format("UPDATE %s SET %s", tableIdentifier(tablePath), setClause);
    }
    String sql = expression.toSQL();
    return String.format("UPDATE %s SET %s WHERE %s", tableIdentifier(tablePath), setClause, sql);
  }

  default String getDeleteSql(DeleteStatement deleteStatement) {
    Expression expression = deleteStatement.getExpression();
    TablePath tablePath = deleteStatement.getTablePath();
    if (Objects.isNull(expression)) {
      return String.format("DELETE FROM %s", tableIdentifier(tablePath));
    }
    String sql = expression.toSQL();
    return String.format("DELETE FROM %s WHERE %s", tableIdentifier(tablePath), sql);
  }

  Optional<String> getUpsertSql(UpsertStatement upsertStatement);

  default String getTruncateTableSql(TablePath tablePath) {
    throw new UnsupportedOperationException();
  }

  String buildPageSql(String originalSql, long offset, long limit);

  default String getRowExistsSql(RowExistsStatement rowExistsStatement) {
    TablePath tablePath = rowExistsStatement.getTablePath();
    Expression expression = rowExistsStatement.getExpression();
    if (Objects.isNull(expression)) {
      return String.format("SELECT 1 FROM %s", tableIdentifier(tablePath));
    }
    return String.format(
        "SELECT 1 FROM %s WHERE %s", tableIdentifier(tablePath), expression.toSQL());
  }
}
