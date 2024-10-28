package io.github.octopus.datos.centro.sql.executor.service.impl;

import com.alibaba.druid.DbType;
import io.github.octopus.datos.centro.sql.executor.JDBCDataSourceProperties;
import io.github.octopus.datos.centro.sql.executor.entity.Table;
import io.github.octopus.datos.centro.sql.executor.mapper.DDLMapper;
import io.github.octopus.datos.centro.sql.executor.service.DataWarehouseDDLService;
import io.github.octopus.datos.centro.sql.model.ColumnInfo;
import io.github.octopus.datos.centro.sql.model.DatabaseInfo;
import io.github.octopus.datos.centro.sql.model.IndexInfo;
import io.github.octopus.datos.centro.sql.model.TableInfo;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DataWarehouseDDLServiceImpl extends AbstractSqlExecutor
    implements DataWarehouseDDLService {

  public DataWarehouseDDLServiceImpl(JDBCDataSourceProperties properties) {
    super(properties);
  }

  @Override
  public void createDatabase(DatabaseInfo databaseInfo) {
    execute(() -> dataWarehouseDDLDao.createDatabase(databaseInfo.getName()));
  }

  @Override
  public void dropDatabase(@NotNull String database) {
    execute(() -> dataWarehouseDDLDao.dropDatabase(database));
  }

  @Override
  public void createTable(TableInfo tableInfo) {
    final DbType dbType = getDbType();
    if (dbType == DbType.starrocks && Objects.isNull(tableInfo.getDistributionInfo())) {
      throw new NullPointerException("distribution info cannot be null when dbtype is doris");
    }
    execute(() -> dataWarehouseDDLDao.createTable(DDLMapper.toTableEntity(tableInfo)));
  }

  @Override
  public void dropTable(@Nullable String database, @NotNull String table) {
    execute(() -> dataWarehouseDDLDao.dropTable(database, table));
  }

  @Override
  public void renameTable(
      @Nullable String database, @NotNull String oldTable, @NotNull String newTable) {
    execute(() -> dataWarehouseDDLDao.renameTable(database, oldTable, newTable));
  }

  @Override
  public void addTableComment(@Nullable String database, @NotNull String table, String comment) {
    execute(() -> dataWarehouseDDLDao.modifyTableComment(database, table, comment));
  }

  @Override
  public void modifyTableComment(@Nullable String database, @NotNull String table, String comment) {
    execute(() -> dataWarehouseDDLDao.modifyTableComment(database, table, comment));
  }

  @Override
  public void removeTableComment(@Nullable String database, @NotNull String table) {
    execute(() -> dataWarehouseDDLDao.modifyTableComment(database, table, BLANK_COMMENT));
  }

  @Override
  public void addColumn(@Nullable String database, @NotNull String table, ColumnInfo columnInfo) {
    addColumns(database, table, Collections.singletonList(columnInfo));
  }

  @Override
  public void addColumns(
      @Nullable String database, @NotNull String table, List<ColumnInfo> columnInfos) {
    execute(
        () ->
            dataWarehouseDDLDao.addColumn(
                Table.builder()
                    .databaseName(database)
                    .tableName(table)
                    .columnDefinitions(DDLMapper.toColumnEntities(columnInfos))
                    .build()));
  }

  @Override
  public void modifyColumn(@Nullable String database, @NotNull String table, ColumnInfo newColumn) {
    execute(
        () ->
            dataWarehouseDDLDao.modifyColumn(database, table, DDLMapper.toColumnEntity(newColumn)));
  }

  @Override
  public void renameColumn(
      @Nullable String database,
      @NotNull String table,
      @NotNull String oldColumn,
      @NotNull String newColumn) {
    execute(() -> dataWarehouseDDLDao.renameColumn(database, table, oldColumn, newColumn));
  }

  @Override
  public void dropColumn(@Nullable String database, @NotNull String table, @NotNull String column) {
    execute(() -> dataWarehouseDDLDao.removeColumn(database, table, column));
  }

  @Override
  public void addColumnComment(
      @Nullable String database, @NotNull String table, ColumnInfo columnInfo) {
    final DbType dbType = getDbType();
    switch (dbType) {
      case mysql:
        modifyColumn(database, table, columnInfo);
        break;
      case starrocks:
        execute(
            () ->
                dataWarehouseDDLDao.modifyColumnComment(
                    database, table, columnInfo.getName(), columnInfo.getComment()));
        break;
      default:
        throw new IllegalStateException(String.format("unsupported dbtype [%s]", dbType));
    }
  }

  @Override
  public void modifyColumnComment(
      @Nullable String database, @NotNull String table, ColumnInfo columnInfo) {
    final DbType dbType = getDbType();
    switch (dbType) {
      case mysql:
        modifyColumn(database, table, columnInfo);
        break;
      case starrocks:
        execute(
            () ->
                dataWarehouseDDLDao.modifyColumnComment(
                    database, table, columnInfo.getName(), columnInfo.getComment()));
        break;
      default:
        throw new IllegalStateException(String.format("unsupported dbtype [%s]", dbType));
    }
  }

  @Override
  public void removeColumnComment(
      @Nullable String database, @NotNull String table, ColumnInfo columnInfo) {
    final DbType dbType = getDbType();
    switch (dbType) {
      case mysql:
        execute(
            () ->
                dataWarehouseDDLDao.modifyColumn(
                    database,
                    table,
                    DDLMapper.toColumnEntity(
                        Optional.of(columnInfo)
                            .map(
                                info ->
                                    ColumnInfo.builder()
                                        .name(info.getName())
                                        .fieldType(info.getFieldType())
                                        .precision(info.getPrecision())
                                        .scale(info.getScale())
                                        .nullable(info.isNullable())
                                        .defaultValue(info.getDefaultValue())
                                        .autoIncrement(info.isAutoIncrement())
                                        .aggregateAlgo(info.getAggregateAlgo())
                                        .comment(BLANK_COMMENT)
                                        .build())
                            .orElseThrow(() -> new NullPointerException("column info is null")))));
        break;
      case starrocks:
        execute(
            () ->
                dataWarehouseDDLDao.modifyColumnComment(
                    database, table, columnInfo.getName(), BLANK_COMMENT));
        break;
      default:
        throw new IllegalStateException(String.format("unsupported dbtype [%s]", dbType));
    }
  }

  @Override
  public void createIndex(@Nullable String database, @NotNull String table, IndexInfo indexInfo) {
    execute(
        () -> dataWarehouseDDLDao.createIndex(database, table, DDLMapper.toIndexEntity(indexInfo)));
  }

  @Override
  public void dropIndex(@Nullable String database, @NotNull String table, @NotNull String index) {
    execute(() -> dataWarehouseDDLDao.dropIndex(database, table, index));
  }
}
