package io.github.octopus.sql.executor.plugin.api.dialect;

import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.DatabaseDefinition;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.core.model.schema.TablePath;

public interface DDLStatement extends SqlStatement {

  default String getCreateDatabaseSql(DatabaseDefinition definition) {
    return "CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(definition.getDatabase());
  }

  default String getDropDatabaseSql(String databaseName) {
    return String.format("DROP DATABASE %s", quoteIdentifier(databaseName));
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

  String getAddColumnCommentSql(TablePath tablePath, String column, String comment);

  String getModifyColumnCommentSql(TablePath tablePath, String column, String comment);

  String getDropColumnCommentSql(TablePath tablePath, String column, String comment);

  String getCreateIndexSql(TablePath tablePath, IndexDefinition indexDefinition);

  String getModifyIndexSql(TablePath tablePath, IndexDefinition indexDefinition);

  String getDropIndexSql(TablePath tablePath, String index);
}
