package io.github.octopus.sql.executor.plugin.api.dialect;

import static java.lang.String.format;

import io.github.octopus.sql.executor.core.model.curd.InsertStatement;
import io.github.octopus.sql.executor.core.model.curd.UpdateStatement;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public interface CurdStatement extends SqlStatement {

  default String getInsertSql(InsertStatement insertStatement) {
    TablePath tablePath = insertStatement.getTablePath();
    List<String> fieldNames = insertStatement.getColumns();
    String columns =
        fieldNames.stream()
            .map(col -> getJdbcDialect().quoteIdentifier(col))
            .collect(Collectors.joining(", "));
    String placeholders =
        fieldNames.stream().map(fieldName -> "?").collect(Collectors.joining(", "));
    return String.format(
        "INSERT INTO %s (%s) VALUES (%s)",
        tablePath.getFullNameWithQuoted(getJdbcDialect().quote()), columns, placeholders);
  }

  default String getUpdateSql(
      TablePath tablePath,
      UpdateStatement updateStatement) {

    fieldNames =
        Arrays.stream(fieldNames)
            .filter(
                fieldName ->
                    isPrimaryKeyUpdated
                        || !Arrays.asList(conditionFields)
                        .contains(fieldName))
            .toArray(String[]::new);

    String setClause =
        Arrays.stream(fieldNames)
            .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
            .collect(Collectors.joining(", "));
    String conditionClause =
        Arrays.stream(conditionFields)
            .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
            .collect(Collectors.joining(" AND "));
    return String.format(
        "UPDATE %s SET %s WHERE %s",
        tableIdentifier(database, tableName), setClause, conditionClause);
  }
}
