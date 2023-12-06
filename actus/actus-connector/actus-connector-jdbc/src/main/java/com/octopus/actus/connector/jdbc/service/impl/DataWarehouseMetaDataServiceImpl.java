package com.octopus.actus.connector.jdbc.service.impl;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.JdbcProperties;
import com.octopus.actus.connector.jdbc.mapper.MetaDataMapper;
import com.octopus.actus.connector.jdbc.model.ColumnMetaInfo;
import com.octopus.actus.connector.jdbc.model.DatabaseInfo;
import com.octopus.actus.connector.jdbc.model.TableMetaInfo;
import com.octopus.actus.connector.jdbc.service.DataWarehouseMetaDataService;
import java.util.List;
import javax.sql.DataSource;

public class DataWarehouseMetaDataServiceImpl extends AbstractSqlExecutor
    implements DataWarehouseMetaDataService {

  public DataWarehouseMetaDataServiceImpl(String name, DataSource dataSource, DbType dbType) {
    super(name, dataSource, dbType);
  }

  public DataWarehouseMetaDataServiceImpl(JdbcProperties properties) {
    super(properties);
  }

  @Override
  public List<DatabaseInfo> getDatabaseInfos() {
    return execute(() -> MetaDataMapper.fromDatabaseMetas(metaDataDao.getDatabaseMetas()));
  }

  @Override
  public DatabaseInfo getDatabaseInfo(String database) {
    return execute(
        () -> MetaDataMapper.fromDatabaseMeta(metaDataDao.getDatabaseMetaBySchema(database)));
  }

  @Override
  public List<TableMetaInfo> getTableInfos() {
    return execute(() -> MetaDataMapper.fromTableMetas(getDbType(), metaDataDao.getTableMetas()));
  }

  @Override
  public List<TableMetaInfo> getTableInfos(String database) {
    return execute(
        () ->
            MetaDataMapper.fromTableMetas(
                getDbType(), metaDataDao.getTableMetasBySchema(database)));
  }

  @Override
  public TableMetaInfo getTableInfo(String database, String table) {
    return execute(
        () ->
            MetaDataMapper.fromTableMeta(
                getDbType(), metaDataDao.getTableMetaBySchemaAndTable(database, table)));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos() {
    return execute(() -> MetaDataMapper.fromColumnMetas(getDbType(), metaDataDao.getColumnMetas()));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database) {
    return execute(
        () ->
            MetaDataMapper.fromColumnMetas(
                getDbType(), metaDataDao.getColumnMetasBySchema(database)));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database, String table) {
    return execute(
        () ->
            MetaDataMapper.fromColumnMetas(
                getDbType(), metaDataDao.getColumnMetasBySchemaAndTable(database, table)));
  }

  @Override
  public ColumnMetaInfo getColumnInfo(String database, String table, String column) {
    return execute(
        () ->
            MetaDataMapper.fromColumnMeta(
                getDbType(),
                metaDataDao.getColumnMetaBySchemaAndTableAndColumn(database, table, column)));
  }
}
