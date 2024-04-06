package io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect;

import static java.lang.String.format;

import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.Schema;
import io.github.shawn.octopus.fluxus.engine.model.type.DataWorkflowFieldTypeParse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.rowset.RowSetMetaDataImpl;
import org.jetbrains.annotations.NotNull;

public interface JdbcDialect {
  String dialectName();

  /**
   * get jdbc meta-information type to seatunnel data type mapper.
   *
   * @return a type mapper for the database
   */
  JdbcDialectTypeMapper getJdbcDialectTypeMapper();

  /** Quotes the identifier for table name or field name */
  default String quoteIdentifier(String identifier) {
    return identifier;
  }

  default String tableIdentifier(String database, String tableName) {
    return quoteIdentifier(database) + "." + quoteIdentifier(tableName);
  }

  /**
   * Constructs the dialects insert statement for a single row. The returned string will be used as
   * a {@link java.sql.PreparedStatement}. Fields in the statement must be in the same order as the
   * {@code fieldNames} parameter.
   *
   * <pre>{@code
   * INSERT INTO table_name (column_name [, ...]) VALUES (value [, ...])
   * }</pre>
   *
   * @return the dialects {@code INSERT INTO} statement.
   */
  default String getInsertIntoStatement(String database, String tableName, String[] fieldNames) {
    String columns =
        Arrays.stream(fieldNames).map(this::quoteIdentifier).collect(Collectors.joining(", "));
    String placeholders =
        Arrays.stream(fieldNames).map(fieldName -> "?").collect(Collectors.joining(", "));
    return format(
        "INSERT INTO %s (%s) VALUES (%s)",
        tableIdentifier(database, tableName), columns, placeholders);
  }

  /**
   * Constructs the dialects update statement for a single row with the given condition. The
   * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
   * must be in the same order as the {@code fieldNames} parameter.
   *
   * <pre>{@code
   * UPDATE table_name SET col = val [, ...] WHERE cond [AND ...]
   * }</pre>
   *
   * @return the dialects {@code UPDATE} statement.
   */
  default String getUpdateStatement(
      String database, String tableName, String[] fieldNames, String[] conditionFields) {
    String setClause =
        Arrays.stream(fieldNames)
            .map(fieldName -> format("%s = ?", quoteIdentifier(fieldName)))
            .collect(Collectors.joining(", "));
    String conditionClause =
        Arrays.stream(conditionFields)
            .map(fieldName -> format("%s = ?", quoteIdentifier(fieldName)))
            .collect(Collectors.joining(" AND "));
    return format(
        "UPDATE %s SET %s WHERE %s",
        tableIdentifier(database, tableName), setClause, conditionClause);
  }

  /**
   * Constructs the dialects delete statement for a single row with the given condition. The
   * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
   * must be in the same order as the {@code fieldNames} parameter.
   *
   * <pre>{@code
   * DELETE FROM table_name WHERE cond [AND ...]
   * }</pre>
   *
   * @return the dialects {@code DELETE} statement.
   */
  default String getDeleteStatement(String database, String tableName, String[] conditionFields) {
    String conditionClause =
        Arrays.stream(conditionFields)
            .map(fieldName -> format("%s = ?", quoteIdentifier(fieldName)))
            .collect(Collectors.joining(" AND "));
    return format("DELETE FROM %s WHERE %s", tableIdentifier(database, tableName), conditionClause);
  }

  /**
   * Generates a query to determine if a row exists in the table. The returned string will be used
   * as a {@link java.sql.PreparedStatement}.
   *
   * <pre>{@code
   * SELECT 1 FROM table_name WHERE cond [AND ...]
   * }</pre>
   *
   * @return the dialects {@code QUERY} statement.
   */
  default String getRowExistsStatement(
      String database, String tableName, String[] conditionFields) {
    String fieldExpressions =
        Arrays.stream(conditionFields)
            .map(field -> format("%s = :%s", quoteIdentifier(field), field))
            .collect(Collectors.joining(" AND "));
    return format(
        "SELECT 1 FROM %s WHERE %s", tableIdentifier(database, tableName), fieldExpressions);
  }

  /**
   * Constructs the dialects upsert statement if supported; such as MySQL's {@code DUPLICATE KEY
   * UPDATE}, or PostgreSQL's {@code ON CONFLICT... DO UPDATE SET..}.
   *
   * <p>If supported, the returned string will be used as a {@link java.sql.PreparedStatement}.
   * Fields in the statement must be in the same order as the {@code fieldNames} parameter.
   *
   * <p>If the dialect does not support native upsert statements, the writer will fallback to {@code
   * SELECT ROW Exists} + {@code UPDATE}/{@code INSERT} which may have poor performance.
   *
   * @return the dialects {@code UPSERT} statement or {@link Optional#empty()}.
   */
  Optional<String> getUpsertStatement(
      String database, String tableName, String[] fieldNames, String[] uniqueKeyFields);

  /**
   * Different dialects optimize their PreparedStatement
   *
   * @return The logic about optimize PreparedStatement
   */
  default PreparedStatement creatPreparedStatement(
      Connection connection, String queryTemplate, int fetchSize) throws SQLException {
    PreparedStatement statement =
        connection.prepareStatement(
            queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
      statement.setFetchSize(fetchSize);
    }
    return statement;
  }

  default ResultSetMetaData getResultSetMetaData(Connection conn, @NotNull String query)
      throws SQLException {
    PreparedStatement ps = conn.prepareStatement(query);
    return ps.getMetaData();
  }

  interface JdbcDialectTypeMapper {
    default List<Schema> mapping(ResultSetMetaData metaData) throws SQLException {
      int columnCount = metaData.getColumnCount();
      List<Schema> schemas = new ArrayList<>(columnCount);
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        int columnType = metaData.getColumnType(i);
        int precision = metaData.getPrecision(i);
        int scale = metaData.getScale(i);
        DataWorkflowFieldType dataWorkflowFieldType =
            DataWorkflowFieldTypeParse.parseSqlType(columnType, precision, scale);
        int nullable = metaData.isNullable(i);
        Schema schema =
            Schema.builder()
                .fieldName(columnName)
                .fieldType(dataWorkflowFieldType)
                .precision(precision)
                .scale(scale)
                .nullable(nullable != RowSetMetaDataImpl.columnNoNulls)
                .build();
        schemas.add(schema);
      }
      return schemas;
    }
  }
}
