package io.github.octopus.actus.plugin.oracle.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.core.model.curd.UpsertStatement;
import io.github.octopus.actus.core.model.schema.ColumnDefinition;
import io.github.octopus.actus.plugin.api.dialect.CurdStatement;
import io.github.octopus.actus.plugin.api.dialect.DialectRegistry;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class OracleCurdStatement implements CurdStatement {

  private static final OracleCurdStatement CURD_STATEMENT = new OracleCurdStatement();

  private OracleCurdStatement() {}

  public static CurdStatement getCurdStatement() {
    return CURD_STATEMENT;
  }

  @Override
  public Optional<String> getUpsertSql(UpsertStatement upsertStatement) {
    List<String> fieldNames =
        upsertStatement
            .getColumns()
            .stream()
            .map(ColumnDefinition::getColumn)
            .collect(Collectors.toList());
    List<String> uniqueKeyFields = upsertStatement.uniqueColumns();
    List<String> nonUniqueKeyFields = upsertStatement.nonUniqueColumns();
    String valuesBinding =
        fieldNames
            .stream()
            .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
            .collect(Collectors.joining(", "));

    String usingClause = String.format("SELECT %s FROM DUAL", valuesBinding);
    String onConditions =
        uniqueKeyFields
            .stream()
            .map(
                fieldName ->
                    String.format(
                        "TARGET.%s=SOURCE.%s",
                        quoteIdentifier(fieldName), quoteIdentifier(fieldName)))
            .collect(Collectors.joining(" AND "));
    String updateSetClause =
        nonUniqueKeyFields
            .stream()
            .map(
                fieldName ->
                    String.format(
                        "TARGET.%s=SOURCE.%s",
                        quoteIdentifier(fieldName), quoteIdentifier(fieldName)))
            .collect(Collectors.joining(", "));
    String insertFields =
        fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
    String insertValues =
        fieldNames
            .stream()
            .map(fieldName -> "SOURCE." + quoteIdentifier(fieldName))
            .collect(Collectors.joining(", "));

    String upsertSQL =
        String.format(
            " MERGE INTO %s TARGET"
                + " USING (%s) SOURCE"
                + " ON (%s) "
                + " WHEN MATCHED THEN"
                + " UPDATE SET %s"
                + " WHEN NOT MATCHED THEN"
                + " INSERT (%s) VALUES (%s)",
            tableIdentifier(upsertStatement.getTablePath()),
            usingClause,
            onConditions,
            updateSetClause,
            insertFields,
            insertValues);

    return Optional.of(upsertSQL);
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.ORACLE);
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
