package io.github.octopus.sql.executor.plugin.sqlserver.dialect;

import io.github.octopus.sql.executor.core.StringPool;
import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintDefinition;
import io.github.octopus.sql.executor.core.model.schema.ConstraintType;
import io.github.octopus.sql.executor.core.model.schema.DatabaseDefinition;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import io.github.octopus.sql.executor.plugin.api.dialect.DDLStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.DialectRegistry;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class SqlServerDDLStatement implements DDLStatement {

  private static final SqlServerDDLStatement DDL_STATEMENT = new SqlServerDDLStatement();

  private SqlServerDDLStatement() {}

  public static DDLStatement getDDLStatement() {
    return DDL_STATEMENT;
  }

  @Override
  public String getCreateDatabaseSql(DatabaseDefinition definition) {
    // 保证幂等性
    return String.format(
        "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '%s')\n"
            + "BEGIN\n"
            + "    CREATE DATABASE %s;\n"
            + "END",
        definition.getDatabase(), quoteIdentifier(definition.getDatabase()));
  }

  @Override
  public String getDropDatabaseSql(String database) {
    // 保证幂等性
    return String.format(
        "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '%s')\n"
            + "BEGIN\n"
            + "    DROP DATABASE %s;\n"
            + "END",
        database, quoteIdentifier(database));
  }

  @Override
  public String getCreateSchemaSql(String database, String schema) {
    return String.format(
        "use %s;\n"
            + "GO\n"
            + "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s')\n"
            + "BEGIN\n"
            + "    CREATE SCHEMA %s;\n"
            + "END",
        quoteIdentifier(database), schema, quoteIdentifier(schema));
  }

  @Override
  public String getDropSchemaSql(String database, String schema) {
    return String.format(
        "use %s;\n"
            + "GO\n"
            + "IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s')\n"
            + "BEGIN\n"
            + "    DROP SCHEMA %s;\n"
            + "END",
        quoteIdentifier(database), schema, quoteIdentifier(schema));
  }

  @Override
  public String getCreateTableSql(TableDefinition tableDefinition) {
    List<String> sqls = new ArrayList<>();
    TablePath tablePath = tableDefinition.getTablePath();
    String sqlTableName = tableIdentifier(tablePath);
    Map<String, String> columnComments = new HashMap<>();
    sqls.add(
        String.format(
            "IF OBJECT_ID('%s', 'U') IS NULL \n" + "BEGIN \n" + "CREATE TABLE %s ( \n%s\n)",
            sqlTableName,
            sqlTableName,
            buildColumnsIdentifySql(
                tableDefinition.getColumns(), tableDefinition.getConstraints(), columnComments)));

    String sqlTableSql = String.join(" ", sqls) + ";";
    StringBuilder tableAndColumnComment = new StringBuilder();
    String comment = tableDefinition.getComment();
    if (StringUtils.isNotBlank(comment)) {
      sqls.add("COMMENT = '" + comment + "'");
      tableAndColumnComment.append(
          String.format(
              "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s';\n",
              tablePath.getDatabaseName(),
              comment,
              tablePath.getSchemaName(),
              tablePath.getTableName()));
    }
    String columnComment =
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s';\n";
    columnComments.forEach(
        (fieldName, com) -> {
          tableAndColumnComment.append(
              String.format(
                  columnComment,
                  tablePath.getDatabaseName(),
                  com,
                  tablePath.getSchemaName(),
                  tablePath.getTableName(),
                  fieldName));
        });
    return String.join("\n", sqlTableSql, tableAndColumnComment.toString(), "END");
  }

  @Override
  public String getRenameTableSql(TablePath oldTablePath, String newTableName) {
    return String.format(
        "EXEC sp_rename '%s', '%s'",
        tableIdentifier(oldTablePath),
        tableIdentifier(
            TablePath.of(
                oldTablePath.getDatabaseName(), oldTablePath.getSchemaName(), newTableName)));
  }

  @Override
  public String getAddTableCommentSql(TablePath tablePath, String comment) {
    return String.format(
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s'",
        tablePath.getDatabaseName(), comment, tablePath.getSchemaName(), tablePath.getTableName());
  }

  @Override
  public String getModifyTableCommentSql(TablePath tablePath, String comment) {
    return String.format(
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s'",
        tablePath.getDatabaseName(), comment, tablePath.getSchemaName(), tablePath.getTableName());
  }

  @Override
  public String getDropTableCommentSql(TablePath tablePath) {
    return String.format(
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s'",
        tablePath.getDatabaseName(),
        StringPool.BLANK_COMMENT,
        tablePath.getSchemaName(),
        tablePath.getTableName());
  }

  @Override
  public String getAddColumnSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    Map<String, String> columnComments = new HashMap<>();
    String columnSql = buildColumnSql(columnDefinition, columnComments);
    if (StringUtils.isBlank(columnDefinition.getComment())) {
      return String.format("ALTER TABLE %s ADD %s", tableIdentifier(tablePath), columnSql);
    }
    String columnComment =
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s';";
    String comment =
        columnComments
            .entrySet()
            .stream()
            .map(
                entry ->
                    String.format(
                        columnComment,
                        tablePath.getDatabaseName(),
                        entry.getValue(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName(),
                        entry.getKey()))
            .collect(Collectors.joining("\n"));
    return String.format(
        "ALTER TABLE %s ADD %s;\n%s", tableIdentifier(tablePath), columnSql, comment);
  }

  @Override
  public String getRenameColumnSql(TablePath tablePath, String oldColumn, String newColumn) {
    return String.format(
        "EXEC sp_rename '%s', '%s'",
        tableIdentifier(tablePath) + "." + quoteIdentifier(oldColumn),
        tableIdentifier(tablePath) + "." + quoteIdentifier(newColumn));
  }

  @Override
  public String getModifyColumnSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    Map<String, String> columnComments = new HashMap<>();
    String columnSql = buildColumnSql(columnDefinition, columnComments);
    if (StringUtils.isBlank(columnDefinition.getComment())) {
      return String.format("ALTER TABLE %s ALTER COLUMN %s", tableIdentifier(tablePath), columnSql);
    }
    String columnComment =
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s';";
    String comment =
        columnComments
            .entrySet()
            .stream()
            .map(
                entry ->
                    String.format(
                        columnComment,
                        tablePath.getDatabaseName(),
                        entry.getValue(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName(),
                        entry.getKey()))
            .collect(Collectors.joining("\n"));
    return String.format(
        "ALTER TABLE %s ALTER COLUMN %s;\n%s", tableIdentifier(tablePath), columnSql, comment);
  }

  @Override
  public String getDropColumnSql(TablePath tablePath, String column) {
    return String.format(
        "ALTER TABLE %s DROP COLUMN %s", tableIdentifier(tablePath), quoteIdentifier(column));
  }

  @Override
  public String getAddColumnCommentSql(
      TablePath tablePath, ColumnDefinition columnDefinition, String comment) {
    String columnComment =
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s';";
    return String.format(
        columnComment,
        tablePath.getDatabaseName(),
        comment,
        tablePath.getSchemaName(),
        tablePath.getTableName(),
        columnDefinition.getColumn());
  }

  @Override
  public String getModifyColumnCommentSql(
      TablePath tablePath, ColumnDefinition columnDefinition, String comment) {
    String columnComment =
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s';";
    return String.format(
        columnComment,
        tablePath.getDatabaseName(),
        comment,
        tablePath.getSchemaName(),
        tablePath.getTableName(),
        columnDefinition.getColumn());
  }

  @Override
  public String getDropColumnCommentSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    String columnComment =
        "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s';";
    return String.format(
        columnComment,
        tablePath.getDatabaseName(),
        StringPool.BLANK_COMMENT,
        tablePath.getSchemaName(),
        tablePath.getTableName(),
        columnDefinition.getColumn());
  }

  @Override
  public String getCreateIndexSql(TablePath tablePath, IndexDefinition indexDefinition) {
    return String.format(
        "CREATE INDEX %s ON %s (%s)",
        quoteIdentifier(indexDefinition.getName()),
        tableIdentifier(tablePath),
        indexDefinition
            .getColumns()
            .stream()
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", ")));
  }

  @Override
  public String getDropIndexSql(TablePath tablePath, String index) {
    return String.format("DROP INDEX %s ON %s", quoteIdentifier(index), tableIdentifier(tablePath));
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.SQLSERVER);
  }

  private String buildColumnsIdentifySql(
      List<ColumnDefinition> columnDefinitions,
      List<ConstraintDefinition> constraintDefinitions,
      Map<String, String> columnComments) {
    List<String> columnSqls = new ArrayList<>();
    for (ColumnDefinition column : columnDefinitions) {
      columnSqls.add("\t" + buildColumnSql(column, columnComments));
    }

    if (CollectionUtils.isNotEmpty(constraintDefinitions)) {
      for (ConstraintDefinition constraintDefinition : constraintDefinitions) {
        columnSqls.add("\t" + buildConstraintSql(constraintDefinition));
      }
    }
    return String.join(", \n", columnSqls);
  }

  private String buildColumnSql(
      ColumnDefinition columnDefinition, Map<String, String> columnComments) {
    List<String> columnSqls = new ArrayList<>();
    String column = quoteIdentifier(columnDefinition.getColumn());
    columnSqls.add(column);
    String columnType = columnDefinition.getColumnType();
    columnSqls.add(columnType);
    if (columnDefinition.isNullable()) {
      columnSqls.add("NULL");
    } else {
      columnSqls.add("NOT NULL");
    }
    if (columnDefinition.getComment() != null) {
      columnComments.put(
          columnDefinition.getColumn(), columnDefinition.getComment().replace("'", "''"));
    }
    return String.join(" ", columnSqls);
  }

  private String buildConstraintSql(ConstraintDefinition constraintDefinition) {
    ConstraintType constraintType = constraintDefinition.getConstraintType();
    String name = constraintDefinition.getName();
    List<String> columns = constraintDefinition.getColumns();
    return String.format(
        "CONSTRAINT %s %s (%s)",
        quoteIdentifier(name),
        constraintType.getType(),
        columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")));
  }
}
