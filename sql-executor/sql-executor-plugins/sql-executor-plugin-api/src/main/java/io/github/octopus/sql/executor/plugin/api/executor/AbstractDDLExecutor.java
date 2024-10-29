package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.SqlSessionProvider;
import io.github.octopus.sql.executor.core.entity.Table;
import io.github.octopus.sql.executor.core.exception.SqlException;
import io.github.octopus.sql.executor.core.exception.SqlExecuteException;
import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.DatabaseDefinition;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.plugin.api.dao.DDLDao;
import io.github.octopus.sql.executor.plugin.api.mapper.DDLMapper;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.session.SqlSessionManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractDDLExecutor implements DDLExecutor {

  protected static final String BLANK_COMMENT = "";

  private final String name;
  private final DDLDao ddlDao;

  protected AbstractDDLExecutor(String name, DataSource dataSource) {
    this.name = name;
    SqlSessionManager sqlSession = SqlSessionProvider.createSqlSession(name, dataSource);
    sqlSession.getConfiguration().addMapper(getDDLDaoClass());
    this.ddlDao = sqlSession.getMapper(getDDLDaoClass());
  }

  protected abstract Class<? extends DDLDao> getDDLDaoClass();

  @Override
  public void createDatabase(DatabaseDefinition databaseInfo) {
    executeDDL(ddlDao -> ddlDao.createDatabase(databaseInfo.getDatabase()));
  }

  @Override
  public void dropDatabase(@NotNull String database) {
    executeDDL(ddlDao -> ddlDao.dropDatabase(database));
  }

  @Override
  public void createTable(TableDefinition tableInfo) {
    executeDDL(ddlDao -> ddlDao.createTable(DDLMapper.toTableEntity(tableInfo)));
  }

  @Override
  public void dropTable(@Nullable String database, @Nullable String schema, @NotNull String table) {
    executeDDL(ddlDao -> ddlDao.dropTable(database, schema, table));
  }

  @Override
  public void renameTable(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String oldTable,
      @NotNull String newTable) {
    executeDDL(ddlDao -> ddlDao.renameTable(database, schema, oldTable, newTable));
  }

  @Override
  public void addTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table, String comment) {

    executeDDL(ddlDao -> ddlDao.modifyTableComment(database, schema, table, comment));
  }

  @Override
  public void modifyTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table, String comment) {
    executeDDL(ddlDao -> ddlDao.modifyTableComment(database, schema, table, comment));
  }

  @Override
  public void removeTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table) {

    executeDDL(ddlDao -> ddlDao.modifyTableComment(database, schema, table, BLANK_COMMENT));
  }

  @Override
  public void addColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {
    addColumns(database, schema, table, Collections.singletonList(columnInfo));
  }

  @Override
  public void addColumns(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      List<ColumnDefinition> columnInfos) {
    executeDDL(
        ddlDao ->
            ddlDao.addColumn(
                Table.builder()
                    .databaseName(database)
                    .tableName(table)
                    .columnDefinitions(DDLMapper.toColumnEntities(columnInfos))
                    .build()));
  }

  @Override
  public void modifyColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition newColumn) {
    executeDDL(
        ddlDao ->
            ddlDao.modifyColumn(database, schema, table, DDLMapper.toColumnEntity(newColumn)));
  }

  @Override
  public void renameColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String oldColumn,
      @NotNull String newColumn) {
    executeDDL(ddlDao -> ddlDao.renameColumn(database, schema, table, oldColumn, newColumn));
  }

  @Override
  public void dropColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String column) {
    executeDDL(ddlDao -> ddlDao.removeColumn(database, schema, table, column));
  }

  @Override
  public void addColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {
    modifyColumn(database, schema, table, columnInfo);
  }

  @Override
  public void modifyColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {
    modifyColumn(database, schema, table, columnInfo);
  }

  @Override
  public void removeColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {
    executeDDL(
        ddlDao ->
            ddlDao.modifyColumnComment(
                database, schema, table, columnInfo.getColumn(), columnInfo.getComment()));
  }

  @Override
  public void createIndex(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      IndexDefinition indexInfo) {
    executeDDL(
        ddlDao -> ddlDao.createIndex(database, schema, table, DDLMapper.toIndexEntity(indexInfo)));
  }

  @Override
  public void createIndexes(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      List<IndexDefinition> indexInfos) {
    if (CollectionUtils.isEmpty(indexInfos)) {
      throw new SqlException("no index need create");
    }
    indexInfos.forEach(indexInfo -> createIndex(database, schema, table, indexInfo));
  }

  @Override
  public void dropIndex(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String index) {
    executeDDL(ddlDao -> ddlDao.dropIndex(database, schema, table, index));
  }

  @Override
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

  @Override
  public void execute(@NotNull String sql) {
    executeDDL(ddlDao -> ddlDao.executeSQL(sql));
  }

  protected void executeDDL(Consumer<DDLDao> ddlDaoConsumer) {
    try {
      ddlDaoConsumer.accept(ddlDao);
    } catch (Exception e) {
      throw new SqlExecuteException(e);
    } finally {
      SqlSessionProvider.releaseSqlSessionManager(name);
    }
  }
}
