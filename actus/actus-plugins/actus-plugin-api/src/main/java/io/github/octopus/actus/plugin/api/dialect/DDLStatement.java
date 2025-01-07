package io.github.octopus.actus.plugin.api.dialect;

import io.github.octopus.actus.core.model.schema.ColumnDefinition;
import io.github.octopus.actus.core.model.schema.DatabaseDefinition;
import io.github.octopus.actus.core.model.schema.IndexDefinition;
import io.github.octopus.actus.core.model.schema.TableDefinition;
import io.github.octopus.actus.core.model.schema.TablePath;

public interface DDLStatement extends SqlStatement {

  default String getCreateDatabaseSql(DatabaseDefinition definition) {
    throw new UnsupportedOperationException();
  }

  default String getDropDatabaseSql(String databaseName) {
    throw new UnsupportedOperationException();
  }

  default String getCreateSchemaSql(String database, String schema) {
    throw new UnsupportedOperationException();
  }

  default String getDropSchemaSql(String database, String schema) {
    throw new UnsupportedOperationException();
  }

  String getCreateTableSql(TableDefinition tableDefinition);

  default String getDropTableSql(TablePath tablePath) {
    return String.format("DROP TABLE %s", tableIdentifier(tablePath));
  }

  String getRenameTableSql(TablePath oldTablePath, String newTableName);

  String getAddTableCommentSql(TablePath tablePath, String comment);

  String getModifyTableCommentSql(TablePath tablePath, String comment);

  String getDropTableCommentSql(TablePath tablePath);

  String getAddColumnSql(TablePath tablePath, ColumnDefinition columnDefinition);

  String getRenameColumnSql(TablePath tablePath, String oldColumn, String newColumn);

  String getModifyColumnSql(TablePath tablePath, ColumnDefinition columnDefinition);

  String getDropColumnSql(TablePath tablePath, String column);

  String getAddColumnCommentSql(
      TablePath tablePath, ColumnDefinition columnDefinition, String comment);

  String getModifyColumnCommentSql(
      TablePath tablePath, ColumnDefinition columnDefinition, String comment);

  String getDropColumnCommentSql(TablePath tablePath, ColumnDefinition columnDefinition);

  String getCreateIndexSql(TablePath tablePath, IndexDefinition indexDefinition);

  String getDropIndexSql(TablePath tablePath, String index);
}
