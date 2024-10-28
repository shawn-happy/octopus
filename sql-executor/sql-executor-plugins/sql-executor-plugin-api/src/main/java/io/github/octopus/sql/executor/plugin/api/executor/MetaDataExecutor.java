package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.model.metadata.ColumnMetaInfo;
import io.github.octopus.sql.executor.core.model.metadata.TableMetaInfo;
import io.github.octopus.sql.executor.core.model.schema.DatabaseInfo;
import io.github.octopus.sql.executor.plugin.api.mapper.MetaDataMapper;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.collections4.CollectionUtils;

public abstract class MetaDataExecutor extends AbstractSqlExecutor {

  public MetaDataExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }

  public List<DatabaseInfo> getDatabaseInfos() {
    return executeMetaData(
        metaData -> {
          List<DatabaseInfo> databaseInfos =
              MetaDataMapper.fromDatabaseMetas(metaData.getDatabaseMetas());
          if (CollectionUtils.isEmpty(databaseInfos)) {
            return new ArrayList<>();
          }
          return databaseInfos;
        });
  }

  public DatabaseInfo getDatabaseInfo(String database) {
    return executeMetaData(
        metaData -> MetaDataMapper.fromDatabaseMeta(metaData.getDatabaseMetaByDatabase(database)));
  }

  public List<TableMetaInfo> getTableInfos() {
    return executeMetaData(
        metaData -> MetaDataMapper.fromTableMetas(getJdbcType(), metaData.getTableMetas()));
  }

  public List<TableMetaInfo> getTableInfos(List<String> database) {
    return executeMetaData(
        metaDataDao ->
            MetaDataMapper.fromTableMetas(
                getJdbcType(), metaDataDao.getTableMetasByDatabases(database)));
  }

  public List<TableMetaInfo> getTableInfos(String database, String schemas) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromTableMetas(
                getJdbcType(), metaData.getTableMetasByDatabaseAndSchema(database, schemas)));
  }

  public TableMetaInfo getTableInfo(String database, String schemas, String table) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromTableMeta(
                getJdbcType(),
                metaData.getTableMetasByDatabaseAndSchemaAndTable(database, schemas, table)));
  }

  public List<ColumnMetaInfo> getColumnInfos() {
    return executeMetaData(
        metaData -> MetaDataMapper.fromColumnMetas(getJdbcType(), metaData.getColumnMetas()));
  }

  public List<ColumnMetaInfo> getColumnInfos(String database, String schemas) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromColumnMetas(
                getJdbcType(), metaData.getColumnMetasByDatabaseAndSchema(database, schemas)));
  }

  public List<ColumnMetaInfo> getColumnInfos(String database, String schemas, String table) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromColumnMetas(
                getJdbcType(),
                metaData.getColumnMetasByDatabaseAndSchemaAndTable(database, schemas, table)));
  }

  public ColumnMetaInfo getColumnInfo(
      String database, String schemas, String table, String column) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromColumnMeta(
                getJdbcType(),
                metaData.getColumnMetasByDatabaseAndSchemaAndTableAndColumn(
                    database, schemas, table, column)));
  }
}
