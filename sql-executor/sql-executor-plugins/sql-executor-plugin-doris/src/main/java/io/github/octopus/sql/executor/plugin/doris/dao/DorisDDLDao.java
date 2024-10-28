package io.github.octopus.sql.executor.plugin.doris.dao;

import io.github.octopus.sql.executor.core.entity.Column;
import io.github.octopus.sql.executor.core.entity.Index;
import io.github.octopus.sql.executor.core.entity.Table;
import io.github.octopus.sql.executor.plugin.api.dao.DDLDao;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface DorisDDLDao extends DDLDao {

  @Override
  void createDatabase(@Param("database") String database);

  @Override
  void dropDatabase(@Param("database") String database);

  @Override
  void createTable(Table table);

  @Override
  void renameTable(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("oldTable") String oldTable,
      @Param("newTable") String newTable);

  @Override
  void modifyTableComment(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("comment") String comment);

  @Override
  void modifyColumnComment(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("column") String column,
      @Param("comment") String comment);

  @Override
  void dropTable(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table);

  @Override
  void addColumn(Table table);

  @Override
  void removeColumn(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("column") String column);

  @Override
  void modifyColumn(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("column") Column column);

  @Override
  void renameColumn(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("oldColumn") String oldColumn,
      @Param("newColumn") String newColumn);

  @Override
  void createIndex(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("index") Index index);

  @Override
  void dropIndex(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("index") String index);

  @Override
  void executeSQL(@Param("sql") String sql);
}
