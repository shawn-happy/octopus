package io.github.octopus.sql.executor.plugin.api.dao;

import io.github.octopus.sql.executor.core.entity.Column;
import io.github.octopus.sql.executor.core.entity.Index;
import io.github.octopus.sql.executor.core.entity.Table;

/** 定义DataWarehouse DDL语句执行 */
public interface DDLDao {

  void createDatabase(String database);

  void dropDatabase(String database);

  void createTable(Table table);

  void renameTable(String database, String schema, String oldTable, String newTable);

  void modifyTableComment(String database, String schema, String table, String comment);

  void modifyColumnComment(
      String database, String schema, String table, String column, String comment);

  void dropTable(String database, String schema, String table);

  void addColumn(Table table);

  void removeColumn(String database, String schema, String oldTable, String column);

  void modifyColumn(String database, String schema, String table, Column column);

  void renameColumn(
      String database, String schema, String table, String oldColumn, String newColumn);

  void createIndex(String database, String schema, String table, Index index);

  void dropIndex(String database, String schema, String table, String index);

  void executeSQL(String sql);
}
