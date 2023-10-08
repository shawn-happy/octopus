package com.octopus.actus.connector.jdbc.dao;

import com.octopus.actus.connector.jdbc.entity.Column;
import com.octopus.actus.connector.jdbc.entity.Index;
import com.octopus.actus.connector.jdbc.entity.Table;

public interface DDLDao {

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
