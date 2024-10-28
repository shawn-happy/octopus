package io.github.octopus.datos.centro.sql.executor.dao;

import io.github.octopus.datos.centro.sql.executor.entity.Column;
import io.github.octopus.datos.centro.sql.executor.entity.Index;
import io.github.octopus.datos.centro.sql.executor.entity.Table;

/** 定义DataWarehouse DDL语句执行 */
public interface DataWarehouseDDLDao {

  void createDatabase(String database);

  void dropDatabase(String database);

  void createTable(Table table);

  void renameTable(String database, String oldTable, String newTable);

  void modifyTableComment(String database, String table, String comment);

  void modifyColumnComment(String database, String table, String column, String comment);

  void dropTable(String database, String table);

  void addColumn(Table table);

  void removeColumn(String database, String oldTable, String column);

  void modifyColumn(String database, String table, Column column);

  void renameColumn(String database, String table, String oldColumn, String newColumn);

  void createIndex(String database, String table, Index index);

  void dropIndex(String database, String table, String index);

  void executeSQL(String sql);
}
