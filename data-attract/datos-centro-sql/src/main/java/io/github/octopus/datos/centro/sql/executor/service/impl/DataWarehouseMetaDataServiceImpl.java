package io.github.octopus.datos.centro.sql.executor.service.impl;

import io.github.octopus.datos.centro.sql.executor.JDBCDataSourceProperties;
import io.github.octopus.datos.centro.sql.executor.mapper.MetaDataMapper;
import io.github.octopus.datos.centro.sql.executor.service.DataWarehouseMetaDataService;
import io.github.octopus.datos.centro.sql.model.ColumnMetaInfo;
import io.github.octopus.datos.centro.sql.model.DatabaseInfo;
import io.github.octopus.datos.centro.sql.model.TableMetaInfo;
import java.util.List;

public class DataWarehouseMetaDataServiceImpl extends AbstractSqlExecutor
    implements DataWarehouseMetaDataService {

  public DataWarehouseMetaDataServiceImpl(JDBCDataSourceProperties properties) {
    super(properties);
  }

  @Override
  public List<DatabaseInfo> getDatabaseInfos() {
    return execute(
        () -> MetaDataMapper.fromDatabaseMetas(dataWarehouseMetaDataDao.getDatabaseMetas()));
  }

  @Override
  public DatabaseInfo getDatabaseInfo(String database) {
    return execute(
        () ->
            MetaDataMapper.fromDatabaseMeta(
                dataWarehouseMetaDataDao.getDatabaseMetaBySchema(database)));
  }

  @Override
  public List<TableMetaInfo> getTableInfos() {
    return execute(
        () -> MetaDataMapper.fromTableMetas(getDbType(), dataWarehouseMetaDataDao.getTableMetas()));
  }

  @Override
  public List<TableMetaInfo> getTableInfos(String database) {
    return execute(
        () ->
            MetaDataMapper.fromTableMetas(
                getDbType(), dataWarehouseMetaDataDao.getTableMetasBySchema(database)));
  }

  @Override
  public TableMetaInfo getTableInfo(String database, String table) {
    return execute(
        () ->
            MetaDataMapper.fromTableMeta(
                getDbType(),
                dataWarehouseMetaDataDao.getTableMetaBySchemaAndTable(database, table)));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos() {
    return execute(
        () ->
            MetaDataMapper.fromColumnMetas(getDbType(), dataWarehouseMetaDataDao.getColumnMetas()));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database) {
    return execute(
        () ->
            MetaDataMapper.fromColumnMetas(
                getDbType(), dataWarehouseMetaDataDao.getColumnMetasBySchema(database)));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database, String table) {
    return execute(
        () ->
            MetaDataMapper.fromColumnMetas(
                getDbType(),
                dataWarehouseMetaDataDao.getColumnMetasBySchemaAndTable(database, table)));
  }

  @Override
  public ColumnMetaInfo getColumnInfo(String database, String table, String column) {
    return execute(
        () ->
            MetaDataMapper.fromColumnMeta(
                getDbType(),
                dataWarehouseMetaDataDao.getColumnMetaBySchemaAndTableAndColumn(
                    database, table, column)));
  }
}
