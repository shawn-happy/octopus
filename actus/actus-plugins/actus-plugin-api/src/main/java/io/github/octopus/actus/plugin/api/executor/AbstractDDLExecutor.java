package io.github.octopus.actus.plugin.api.executor;

import io.github.octopus.actus.core.model.schema.ColumnDefinition;
import io.github.octopus.actus.core.model.schema.DatabaseDefinition;
import io.github.octopus.actus.core.model.schema.IndexDefinition;
import io.github.octopus.actus.core.model.schema.TableDefinition;
import io.github.octopus.actus.core.model.schema.TablePath;
import io.github.octopus.actus.plugin.api.dialect.DDLStatement;

import java.util.List;
import javax.sql.DataSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractDDLExecutor extends AbstractExecutor implements DDLExecutor {

  private final DDLStatement ddlStatement;

  protected AbstractDDLExecutor(DataSource dataSource, DDLStatement ddlStatement) {
    super(dataSource);
    this.ddlStatement = ddlStatement;
  }

  @Override
  public void createDatabase(DatabaseDefinition databaseInfo) {
    String createDatabaseSql = ddlStatement.getCreateDatabaseSql(databaseInfo);
    getProcessor().execute(createDatabaseSql);
  }

  @Override
  public void dropDatabase(@NotNull String database) {
    getProcessor().execute(ddlStatement.getDropDatabaseSql(database));
  }

  @Override
  public void createTable(TableDefinition tableInfo) {
    getProcessor().execute(ddlStatement.getCreateTableSql(tableInfo));
  }

  @Override
  public void dropTable(@Nullable String database, @Nullable String schema, @NotNull String table) {
    getProcessor().execute(ddlStatement.getDropTableSql(TablePath.of(database, schema, table)));
  }

  @Override
  public void renameTable(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String oldTable,
      @NotNull String newTable) {
    getProcessor()
        .execute(
            ddlStatement.getRenameTableSql(TablePath.of(database, schema, oldTable), newTable));
  }

  @Override
  public void addTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table, String comment) {}

  @Override
  public void modifyTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table, String comment) {}

  @Override
  public void removeTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table) {}

  @Override
  public void addColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {}

  @Override
  public void addColumns(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      List<ColumnDefinition> columnInfos) {}

  @Override
  public void modifyColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition newColumn) {}

  @Override
  public void renameColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String oldColumn,
      @NotNull String newColumn) {}

  @Override
  public void dropColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String column) {}

  @Override
  public void addColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {}

  @Override
  public void modifyColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {}

  @Override
  public void removeColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo) {}

  @Override
  public void createIndex(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      IndexDefinition indexInfo) {}

  @Override
  public void createIndexes(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      List<IndexDefinition> indexInfos) {}

  @Override
  public void dropIndex(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String index) {}

  @Override
  public void dropIndexes(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull List<String> indexes) {}

  @Override
  public void execute(@NotNull String sql) {}
}
