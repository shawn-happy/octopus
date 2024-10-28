package io.github.octopus.sql.executor.plugin.sqlserver.dao;

import io.github.octopus.sql.executor.core.entity.ColumnMeta;
import io.github.octopus.sql.executor.core.entity.ConstraintKeyMeta;
import io.github.octopus.sql.executor.core.entity.DatabaseMeta;
import io.github.octopus.sql.executor.core.entity.SchemaMeta;
import io.github.octopus.sql.executor.core.entity.TableMeta;
import io.github.octopus.sql.executor.plugin.api.dao.MetaDataDao;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.annotations.Param;

public interface SqlServerMetaDataDao extends MetaDataDao {
  @Override
  List<DatabaseMeta> getDatabaseMetas();

  @Override
  DatabaseMeta getDatabaseMetaByDatabase(@Param("database") String database);

  @Override
  List<SchemaMeta> getSchemaMetas(@Param("database") String database);

  @Override
  SchemaMeta getSchemaMetaBySchema(
      @Param("database") String database, @Param("schema") String schema);

  @Override
  default List<TableMeta> getTableMetas() {
    List<DatabaseMeta> databaseMetas = getDatabaseMetas();
    if (CollectionUtils.isEmpty(databaseMetas)) {
      return null;
    }
    return databaseMetas
        .stream()
        .flatMap(
            databaseMeta -> getTableMetasByDatabaseAndSchema(databaseMeta.getName(), null).stream())
        .collect(Collectors.toList());
  }

  @Override
  default List<TableMeta> getTableMetasByDatabases(List<String> databases) {
    if (CollectionUtils.isEmpty(databases)) {
      return null;
    }
    return databases
        .stream()
        .flatMap(database -> getTableMetasByDatabaseAndSchema(database, null).stream())
        .collect(Collectors.toList());
  }

  @Override
  List<TableMeta> getTableMetasByDatabaseAndSchema(
      @Param("database") String database, @Param("schema") String schema);

  @Override
  TableMeta getTableMetasByDatabaseAndSchemaAndTable(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table);

  @Override
  default List<ColumnMeta> getColumnMetas() {
    List<DatabaseMeta> databaseMetas = getDatabaseMetas();
    if (CollectionUtils.isEmpty(databaseMetas)) {
      return null;
    }
    return databaseMetas
        .stream()
        .flatMap(
            databaseMeta ->
                getColumnMetasByDatabaseAndSchema(databaseMeta.getName(), null).stream())
        .collect(Collectors.toList());
  }

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
  default List<ConstraintKeyMeta> getConstraintMetas() {
    List<DatabaseMeta> databaseMetas = getDatabaseMetas();
    if (CollectionUtils.isEmpty(databaseMetas)) {
      return null;
    }
    return databaseMetas
        .stream()
        .flatMap(
            databaseMeta ->
                getConstraintMetasByDatabaseAndSchema(databaseMeta.getName(), null).stream())
        .collect(Collectors.toList());
  }

  @Override
  List<ConstraintKeyMeta> getConstraintMetasByDatabaseAndSchema(
      @Param("database") String database, @Param("schema") String schema);

  @Override
  List<ConstraintKeyMeta> getConstraintMetasByDatabaseAndSchemaAndTable(
      @Param("database") String database,
      @Param("schema") String schema,
      @Param("table") String table);
}
