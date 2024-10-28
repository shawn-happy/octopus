package io.github.octopus.sql.executor.plugin.doris.dao;

import io.github.octopus.sql.executor.core.entity.ColumnMeta;
import io.github.octopus.sql.executor.core.entity.ConstraintKeyMeta;
import io.github.octopus.sql.executor.core.entity.DatabaseMeta;
import io.github.octopus.sql.executor.core.entity.TableMeta;
import io.github.octopus.sql.executor.plugin.api.dao.MetaDataDao;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface DorisMetaDataDao extends MetaDataDao {

  @Override
  List<DatabaseMeta> getDatabaseMetas();

  @Override
  DatabaseMeta getDatabaseMetaByDatabase(@Param("database") String database);

  @Override
  List<TableMeta> getTableMetas();

  @Override
  List<TableMeta> getTableMetasByDatabases(List<String> databases);

  @Override
  List<TableMeta> getTableMetasByDatabaseAndSchema(
      @Param("database") String database, @Param("schema") String schema);

  @Override
  TableMeta getTableMetasByDatabaseAndSchemaAndTable(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table);

  @Override
  List<ColumnMeta> getColumnMetas();

  @Override
  List<ColumnMeta> getColumnMetasByDatabaseAndSchema(
      @Param("database") String database, @Param("schema") String schema);

  @Override
  List<ColumnMeta> getColumnMetasByDatabaseAndSchemaAndTable(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table);

  @Override
  ColumnMeta getColumnMetasByDatabaseAndSchemaAndTableAndColumn(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table,
      @Param("column") String columnName);

  @Override
  List<ConstraintKeyMeta> getConstraintMetas();

  @Override
  List<ConstraintKeyMeta> getConstraintMetasByDatabaseAndSchema(
      @Param("database") String database, @Param("schema") String schema);

  @Override
  List<ConstraintKeyMeta> getConstraintMetasByDatabaseAndSchemaAndTable(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table);
}
