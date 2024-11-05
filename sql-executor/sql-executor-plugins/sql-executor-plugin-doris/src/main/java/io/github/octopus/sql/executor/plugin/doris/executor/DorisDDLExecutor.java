package io.github.octopus.sql.executor.plugin.doris.executor;

import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.doris.dao.DorisDDLDao;
import javax.sql.DataSource;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@Getter
public class DorisDDLExecutor extends AbstractDDLExecutor {

  private final Class<? extends DDLDao> dDLDaoClass = DorisDDLDao.class;

  public DorisDDLExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }

  @Override
  public void addColumnComment(
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
  public void modifyColumnComment(
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
  public void removeColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {
    executeDDL(
        ddlDao ->
            ddlDao.modifyColumnComment(
                database, schema, table, columnInfo.getColumn(), BLANK_COMMENT));
  }
}
