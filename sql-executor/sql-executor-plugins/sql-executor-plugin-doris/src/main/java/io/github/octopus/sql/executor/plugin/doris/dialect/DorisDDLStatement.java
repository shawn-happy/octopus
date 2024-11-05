package io.github.octopus.sql.executor.plugin.doris.dialect;

import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.core.model.schema.TablePath;
import io.github.octopus.sql.executor.plugin.api.dialect.DDLStatement;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;

public class DorisDDLStatement implements DDLStatement {

  private static final DorisDDLStatement DDL_STATEMENT = new DorisDDLStatement();

  private DorisDDLStatement() {}

  public static DDLStatement getDDLStatement() {
    return DDL_STATEMENT;
  }

  @Override
  public String getCreateTableSql(TableDefinition tableDefinition) {
    return "";
  }

  @Override
  public String getRenameTableSql(TablePath oldTablePath, String newTableName) {
    return "";
  }

  @Override
  public String getAddTableCommentSql(TablePath tablePath, String comment) {
    return "";
  }

  @Override
  public String getModifyTableCommentSql(TablePath tablePath, String comment) {
    return "";
  }

  @Override
  public String getDropTableCommentSql(TablePath tablePath) {
    return "";
  }

  @Override
  public String getAddColumnSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    return "";
  }

  @Override
  public String getRenameColumnSql(TablePath tablePath, String oldColumn, String newColumn) {
    return "";
  }

  @Override
  public String getModifyColumnSql(TablePath tablePath, ColumnDefinition columnDefinition) {
    return "";
  }

  @Override
  public String getDropColumnSql(TablePath tablePath, String column) {
    return "";
  }

  @Override
  public String getAddColumnCommentSql(TablePath tablePath, String column, String comment) {
    return "";
  }

  @Override
  public String getModifyColumnCommentSql(TablePath tablePath, String column, String comment) {
    return "";
  }

  @Override
  public String getDropColumnCommentSql(TablePath tablePath, String column, String comment) {
    return "";
  }

  @Override
  public String getCreateIndexSql(TablePath tablePath, IndexDefinition indexDefinition) {
    return "";
  }

  @Override
  public String getModifyIndexSql(TablePath tablePath, IndexDefinition indexDefinition) {
    return "";
  }

  @Override
  public String getDropIndexSql(TablePath tablePath, String index) {
    return "";
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return null;
  }
}
