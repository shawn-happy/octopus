package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.entity.Table;
import io.github.octopus.sql.executor.core.exception.SqlException;
import io.github.octopus.sql.executor.core.model.schema.ColumnInfo;
import io.github.octopus.sql.executor.core.model.schema.DatabaseInfo;
import io.github.octopus.sql.executor.core.model.schema.IndexInfo;
import io.github.octopus.sql.executor.core.model.schema.TableInfo;
import io.github.octopus.sql.executor.plugin.api.mapper.DDLMapper;
import java.util.Collections;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class DDLExecutor extends AbstractSqlExecutor {

  protected DDLExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }

  public void createDatabase(DatabaseInfo databaseInfo) {
    executeDDL(ddlDao -> ddlDao.createDatabase(databaseInfo.getName()));
  }

  public void dropDatabase(@NotNull String database) {
    executeDDL(ddlDao -> ddlDao.dropDatabase(database));
  }

  public void createTable(TableInfo tableInfo) {
    executeDDL(ddlDao -> ddlDao.createTable(DDLMapper.toTableEntity(tableInfo)));
  }

  public void dropTable(@Nullable String database, @Nullable String schema, @NotNull String table) {
    executeDDL(ddlDao -> ddlDao.dropTable(database, schema, table));
  }

  public void renameTable(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String oldTable,
      @NotNull String newTable) {
    executeDDL(ddlDao -> ddlDao.renameTable(database, schema, oldTable, newTable));
  }

  public void addTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table, String comment) {

    executeDDL(ddlDao -> ddlDao.modifyTableComment(database, schema, table, comment));
  }

  public void modifyTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table, String comment) {
    executeDDL(ddlDao -> ddlDao.modifyTableComment(database, schema, table, comment));
  }

  public void removeTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table) {

    executeDDL(ddlDao -> ddlDao.modifyTableComment(database, schema, table, BLANK_COMMENT));
  }

  public void addColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnInfo columnInfo) {
    addColumns(database, schema, table, Collections.singletonList(columnInfo));
  }

  public void addColumns(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      List<ColumnInfo> columnInfos) {
    executeDDL(
        ddlDao ->
            ddlDao.addColumn(
                Table.builder()
                    .databaseName(database)
                    .tableName(table)
                    .columnDefinitions(DDLMapper.toColumnEntities(columnInfos))
                    .build()));
  }

  public void modifyColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnInfo newColumn) {
    executeDDL(
        ddlDao ->
            ddlDao.modifyColumn(database, schema, table, DDLMapper.toColumnEntity(newColumn)));
  }

  public void renameColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String oldColumn,
      @NotNull String newColumn) {
    executeDDL(ddlDao -> ddlDao.renameColumn(database, schema, table, oldColumn, newColumn));
  }

  public void dropColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String column) {
    executeDDL(ddlDao -> ddlDao.removeColumn(database, schema, table, column));
  }

  public void addColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnInfo columnInfo) {
    executeDDL(
        ddlDao ->
            ddlDao.modifyColumnComment(
                database, schema, table, columnInfo.getName(), columnInfo.getComment()));
  }

  public void modifyColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnInfo columnInfo) {
    executeDDL(
        ddlDao ->
            ddlDao.modifyColumnComment(
                database, schema, table, columnInfo.getName(), columnInfo.getComment()));
  }

  public void removeColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnInfo columnInfo) {
    executeDDL(
        ddlDao ->
            ddlDao.modifyColumnComment(
                database, schema, table, columnInfo.getName(), columnInfo.getComment()));
  }

  public void createIndex(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      IndexInfo indexInfo) {
    executeDDL(
        ddlDao -> ddlDao.createIndex(database, schema, table, DDLMapper.toIndexEntity(indexInfo)));
  }

  public void createIndexes(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      List<IndexInfo> indexInfos) {
    if (CollectionUtils.isEmpty(indexInfos)) {
      throw new SqlException("no index need create");
    }
    indexInfos.forEach(indexInfo -> createIndex(database, schema, table, indexInfo));
  }

  public void dropIndex(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String index) {
    executeDDL(ddlDao -> ddlDao.dropIndex(database, schema, table, index));
  }

  public void dropIndexes(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull List<String> indexes) {
    if (CollectionUtils.isEmpty(indexes)) {
      return;
    }
    indexes.forEach(index -> dropIndex(database, schema, table, index));
  }

  public void execute(@NotNull String sql) {
    executeDDL(ddlDao -> ddlDao.executeSQL(sql));
  }
}
