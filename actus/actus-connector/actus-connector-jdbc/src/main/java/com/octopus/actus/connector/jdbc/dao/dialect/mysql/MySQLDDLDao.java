package com.octopus.actus.connector.jdbc.dao.dialect.mysql;

import com.octopus.actus.connector.jdbc.dao.DDLDao;
import com.octopus.actus.connector.jdbc.entity.Column;
import com.octopus.actus.connector.jdbc.entity.Index;
import com.octopus.actus.connector.jdbc.entity.Table;
import org.apache.ibatis.annotations.Param;

public interface MySQLDDLDao extends DDLDao {
  @Override
  void createDatabase(@Param("database") String database);

  @Override
  void dropDatabase(@Param("database") String database);

  @Override
  void createTable(Table table);

  @Override
  void renameTable(
      @Param("database") String database,
      @Param("oldTable") String oldTable,
      @Param("newTable") String newTable);

  @Override
  void modifyTableComment(
      @Param("database") String database,
      @Param("table") String table,
      @Param("comment") String comment);

  @Override
  void dropTable(@Param("database") String database, @Param("table") String table);

  @Override
  void addColumn(Table table);

  @Override
  void removeColumn(
      @Param("database") String database,
      @Param("table") String table,
      @Param("column") String column);

  @Override
  void modifyColumn(
      @Param("database") String database,
      @Param("table") String table,
      @Param("column") Column column);

  @Override
  void renameColumn(
      @Param("database") String database,
      @Param("table") String table,
      @Param("oldColumn") String oldColumn,
      @Param("newColumn") String newColumn);

  @Override
  void createIndex(
      @Param("database") String database,
      @Param("table") String table,
      @Param("index") Index index);

  @Override
  void dropIndex(
      @Param("database") String database,
      @Param("table") String table,
      @Param("index") String index);

  @Override
  void executeSQL(@Param("sql") String sql);
}
