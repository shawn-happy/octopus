package io.github.octopus.sql.executor.plugin.sqlserver.dialect;

import io.github.octopus.sql.executor.core.StringPool;
import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.curd.UpsertStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.CurdStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.DialectRegistry;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class SqlServerCurdStatement implements CurdStatement {

  private static final SqlServerCurdStatement CURD_STATEMENT = new SqlServerCurdStatement();

  private SqlServerCurdStatement() {}

  public static CurdStatement getCurdStatement() {
    return CURD_STATEMENT;
  }

  @Override
  public Optional<String> getUpsertSql(UpsertStatement upsertStatement) {
    List<String> fieldNames = upsertStatement.fullColumns();
    List<String> nonUniqueKeyFields = upsertStatement.nonUniqueColumns();
    String valuesBinding =
        fieldNames
            .stream()
            .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
            .collect(Collectors.joining(", "));
    List<String> uniqueKeyFields = upsertStatement.uniqueColumns();
    String usingClause = String.format("SELECT %s", valuesBinding);
    String onConditions =
        uniqueKeyFields
            .stream()
            .map(
                fieldName ->
                    String.format(
                        "[TARGET].%s=[SOURCE].%s",
                        quoteIdentifier(fieldName), quoteIdentifier(fieldName)))
            .collect(Collectors.joining(" AND "));
    String updateSetClause =
        nonUniqueKeyFields
            .stream()
            .map(
                fieldName ->
                    String.format(
                        "[TARGET].%s=[SOURCE].%s",
                        quoteIdentifier(fieldName), quoteIdentifier(fieldName)))
            .collect(Collectors.joining(", "));
    String insertFields =
        fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
    String insertValues =
        fieldNames
            .stream()
            .map(fieldName -> "[SOURCE]." + quoteIdentifier(fieldName))
            .collect(Collectors.joining(", "));
    String upsertSQL =
        String.format(
            "MERGE INTO %s AS [TARGET]"
                + " USING (%s) AS [SOURCE]"
                + " ON (%s)"
                + " WHEN MATCHED THEN"
                + " UPDATE SET %s"
                + " WHEN NOT MATCHED THEN"
                + " INSERT (%s) VALUES (%s);",
            tableIdentifier(upsertStatement.getTablePath()),
            usingClause,
            onConditions,
            updateSetClause,
            insertFields,
            insertValues);
    return Optional.of(upsertSQL);
  }

  @Override
  public String buildPageSql(String originalSql, long offset, long limit) {
    StringBuilder pagingBuilder = new StringBuilder();
    String orderby = getOrderByPart(originalSql);
    String distinctStr = StringPool.EMPTY;

    String loweredString = originalSql.toLowerCase();
    String sqlPartString = originalSql;
    if (loweredString.trim().startsWith("select")) {
      int index = 6;
      if (loweredString.startsWith("select distinct")) {
        distinctStr = "DISTINCT ";
        index = 15;
      }
      sqlPartString = sqlPartString.substring(index);
    }
    pagingBuilder.append(sqlPartString);

    // if no ORDER BY is specified use fake ORDER BY field to avoid errors
    if (StringUtils.isBlank(orderby)) {
      orderby = "ORDER BY CURRENT_TIMESTAMP";
    }
    long firstParam = offset + 1;
    long secondParam = offset + limit;
    return "WITH selectTemp AS (SELECT "
        + distinctStr
        + "TOP 100 PERCENT "
        + " ROW_NUMBER() OVER ("
        + orderby
        + ") as __row_number__, "
        + pagingBuilder
        + ") SELECT * FROM selectTemp WHERE __row_number__ BETWEEN "
        +
        // FIX#299：原因：mysql中limit 10(offset,size) 是从第10开始（不包含10）,；而这里用的BETWEEN是两边都包含，所以改为offset+1
        firstParam
        + " AND "
        + secondParam
        + " ORDER BY __row_number__";
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.SQLSERVER);
  }

  private static String getOrderByPart(String sql) {
    String loweredString = sql.toLowerCase();
    int orderByIndex = loweredString.indexOf("order by");
    if (orderByIndex != -1) {
      return sql.substring(orderByIndex);
    } else {
      return StringPool.EMPTY;
    }
  }
}
