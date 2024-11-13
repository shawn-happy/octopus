package io.github.octopus.sql.executor.plugin.oracle.dialect;

import static io.github.octopus.sql.executor.core.StringPool.BLANK_COMMENT;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintType;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import io.github.octopus.sql.executor.plugin.api.dialect.DDLStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.DialectRegistry;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public class OracleDDLStatement implements DDLStatement {

  private static final OracleDDLStatement DDL_STATEMENT = new OracleDDLStatement();

  private OracleDDLStatement() {}

  public static DDLStatement getDDLStatement() {
    return DDL_STATEMENT;
  }

  @Override
  public String getCreateTableSql(TableDefinition tableDefinition) {
    TablePath tablePath = tableDefinition.getTablePath();
    StringBuilder createTableSql = new StringBuilder();
    createTableSql.append("CREATE TABLE ").append(tableIdentifier(tablePath)).append(" (\n");
    List<ColumnDefinition> columns = tableDefinition.getColumns();
    List<ConstraintDefinition> constraints = tableDefinition.getConstraints();
    List<String> columnSqls =
        columns.stream().map(this::buildColumnSql).collect(Collectors.toList());

    // Add primary key directly in the create table statement
    if (CollectionUtils.isNotEmpty(constraints)) {
      List<String> constraintSqls =
          constraints.stream().map(this::buildConstraintSql).collect(Collectors.toList());
      columnSqls.addAll(constraintSqls);
    }
    createTableSql.append(String.join(",\n", columnSqls));
    createTableSql.append("\n)");
    return createTableSql.toString();
  }

  @Override
  public String getRenameTableSql(TablePath oldTablePath, String newTableName) {
    return String.format(
        "RENAME TABLE %s TO %s",
        tableIdentifier(oldTablePath),
        tableIdentifier(TablePath.of(oldTablePath.getDatabaseName(), newTableName)));
  }

  @Override
  public String getAddTableCommentSql(TablePath tablePath, String comment) {
    return String.format(
        "COMMENT ON TABLE %s is '%s'", tableIdentifier(tablePath), comment.replace("'", "''"));
  }

  @Override
  public String getModifyTableCommentSql(TablePath tablePath, String comment) {
    return String.format(
        "COMMENT ON TABLE %s is '%s'", tableIdentifier(tablePath), comment.replace("'", "''"));
  }

  @Override
  public String getDropTableCommentSql(TablePath tablePath) {
    return String.format("COMMENT ON TABLE %s is '%s'", tableIdentifier(tablePath), BLANK_COMMENT);
  }

  @Override
  public String getAddColumnSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    String columnSql = buildColumnSql(columnDefinition);
    return String.format("ALTER TABLE %s ADD %s", tableIdentifier(tablePath), columnSql);
  }

  @Override
  public String getRenameColumnSql(TablePath tablePath, String oldColumn, String newColumn) {
    return String.format(
        "ALTER TABLE %s RENAME COLUMN %s TO %s",
        tableIdentifier(tablePath), quoteIdentifier(oldColumn), quoteIdentifier(newColumn));
  }

  @Override
  public String getModifyColumnSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    String columnSql = buildColumnSql(columnDefinition);
    return String.format("ALTER TABLE %s MODIFY %s", tableIdentifier(tablePath), columnSql);
  }

  @Override
  public String getDropColumnSql(TablePath tablePath, String column) {
    return String.format(
        "ALTER TABLE %s DROP COLUMN %s", tableIdentifier(tablePath), quoteIdentifier(column));
  }

  @Override
  public String getAddColumnCommentSql(
      TablePath tablePath, ColumnDefinition columnDefinition, String comment) {
    columnDefinition.setComment(comment);
    return buildColumnCommentSql(columnDefinition, tablePath);
  }

  @Override
  public String getModifyColumnCommentSql(
      TablePath tablePath, ColumnDefinition columnDefinition, String comment) {
    columnDefinition.setComment(comment);
    return buildColumnCommentSql(columnDefinition, tablePath);
  }

  @Override
  public String getDropColumnCommentSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    columnDefinition.setComment(BLANK_COMMENT);
    return buildColumnCommentSql(columnDefinition, tablePath);
  }

  @Override
  public String getCreateIndexSql(TablePath tablePath, IndexDefinition indexDefinition) {
    return String.format(
        "CREATE %s INDEX %s ON %s (%s)",
        indexDefinition.getIndexAlgo().getAlgo(),
        indexDefinition.getName(),
        tableIdentifier(tablePath),
        indexDefinition
            .getColumns()
            .stream()
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", ")));
  }

  @Override
  public String getDropIndexSql(TablePath tablePath, String index) {
    return String.format(
        "DROP INDEX %s.%s",
        tablePath.getDatabaseNameWithQuoted(
            getJdbcDialect().quoteLeft(),
            getJdbcDialect().quoteRight(),
            getJdbcDialect().getFieldIde()),
        quoteIdentifier(index));
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.ORACLE);
  }

  private String buildColumnSql(ColumnDefinition column) {
    StringBuilder columnSql = new StringBuilder();
    columnSql.append(quoteIdentifier(column.getColumn()));
    FieldType fieldType = column.getFieldType();
    Integer precision = column.getPrecision();
    Integer scale = column.getScale();
    String type = fieldType.getDataType();
    if (precision != null && scale != null) {
      type = String.format("%s(%s, %s)", fieldType.getDataType(), precision, scale);
    } else if (precision != null) {
      type = String.format("%s(%s)", fieldType.getDataType(), precision);
    }
    columnSql.append(type);

    if (!column.isNullable()) {
      columnSql.append(" NOT NULL");
    }

    return columnSql.toString();
  }

  private String buildConstraintSql(ConstraintDefinition constraintDefinition) {
    if (constraintDefinition == null) {
      return null;
    }
    String name = constraintDefinition.getName();
    ConstraintType constraintType = constraintDefinition.getConstraintType();
    String columns =
        constraintDefinition
            .getColumns()
            .stream()
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", "));
    return String.format(
        "CONSTRAINT %s %s (%s)", quoteIdentifier(name), constraintType.getType(), columns);
  }

  private String buildColumnCommentSql(ColumnDefinition column, TablePath tablePath) {
    return "COMMENT ON COLUMN "
        + tableIdentifier(tablePath)
        + "."
        + quoteIdentifier(column.getColumn())
        + " IS '"
        + column.getComment().replace("'", "''")
        + "'";
  }
}
