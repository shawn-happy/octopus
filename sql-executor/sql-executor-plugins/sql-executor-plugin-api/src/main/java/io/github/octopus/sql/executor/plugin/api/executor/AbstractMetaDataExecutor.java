package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.exception.SqlExecuteException;
import io.github.octopus.sql.executor.core.model.metadata.ColumnMetaInfo;
import io.github.octopus.sql.executor.core.model.metadata.DatabaseMetaInfo;
import io.github.octopus.sql.executor.core.model.metadata.TableMetaInfo;
import io.github.octopus.sql.executor.plugin.api.mapper.MetaDataMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.session.SqlSessionManager;

public abstract class AbstractMetaDataExecutor implements MetaDataExecutor {

  private final String name;
  private final MetaDataDao metaDataDao;

  protected AbstractMetaDataExecutor(String name, DataSource dataSource) {
    this.name = name;
    SqlSessionManager sqlSession = SqlSessionProvider.createSqlSession(name, dataSource);
    sqlSession.getConfiguration().addMapper(getMetaDataDaoClass());
    this.metaDataDao = sqlSession.getMapper(getMetaDataDaoClass());
  }

  protected abstract Class<? extends MetaDataDao> getMetaDataDaoClass();

  @Override
  public List<DatabaseMetaInfo> getDatabaseInfos() {
    return executeMetaData(
        metaData -> {
          List<DatabaseMetaInfo> databaseInfos =
              MetaDataMapper.fromDatabaseMetas(metaData.getDatabaseMetas());
          if (CollectionUtils.isEmpty(databaseInfos)) {
            return new ArrayList<>();
          }
          return databaseInfos;
        });
  }

  @Override
  public DatabaseMetaInfo getDatabaseInfo(String database) {
    return executeMetaData(
        metaData -> MetaDataMapper.fromDatabaseMeta(metaData.getDatabaseMetaByDatabase(database)));
  }

  @Override
  public List<TableMetaInfo> getTableInfos() {
    return executeMetaData(
        metaData -> MetaDataMapper.fromTableMetas(null, metaData.getTableMetas()));
  }

  @Override
  public List<TableMetaInfo> getTableInfos(List<String> database) {
    return executeMetaData(
        metaDataDao ->
            MetaDataMapper.fromTableMetas(null, metaDataDao.getTableMetasByDatabases(database)));
  }

  @Override
  public List<TableMetaInfo> getTableInfos(String database, String schemas) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromTableMetas(
                null, metaData.getTableMetasByDatabaseAndSchema(database, schemas)));
  }

  @Override
  public TableMetaInfo getTableInfo(String database, String schemas, String table) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromTableMeta(
                null, metaData.getTableMetasByDatabaseAndSchemaAndTable(database, schemas, table)));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos() {
    return executeMetaData(
        metaData -> MetaDataMapper.fromColumnMetas(null, metaData.getColumnMetas()));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database, String schemas) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromColumnMetas(
                null, metaData.getColumnMetasByDatabaseAndSchema(database, schemas)));
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database, String schemas, String table) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromColumnMetas(
                null,
                metaData.getColumnMetasByDatabaseAndSchemaAndTable(database, schemas, table)));
  }

  @Override
  public ColumnMetaInfo getColumnInfo(
      String database, String schemas, String table, String column) {
    return executeMetaData(
        metaData ->
            MetaDataMapper.fromColumnMeta(
                null,
                metaData.getColumnMetasByDatabaseAndSchemaAndTableAndColumn(
                    database, schemas, table, column)));
  }

  private <R> R executeMetaData(Function<MetaDataDao, R> function) {
    try {
      return function.apply(metaDataDao);
    } catch (Exception e) {
      throw new SqlExecuteException(e);
    } finally {
      SqlSessionProvider.releaseSqlSessionManager(name);
    }
  }
}
