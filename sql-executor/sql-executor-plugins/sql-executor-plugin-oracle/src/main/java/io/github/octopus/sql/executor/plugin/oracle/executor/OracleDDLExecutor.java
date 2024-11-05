package io.github.octopus.sql.executor.plugin.oracle.executor;

import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.DatabaseDefinition;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.oracle.dialect.OracleDDLStatement;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@Getter
public class OracleDDLExecutor extends AbstractDDLExecutor {

  public OracleDDLExecutor(DataSource dataSource) {
    super(dataSource, OracleDDLStatement.getDDLStatement());
  }

  @Override
  public void createDatabase(DatabaseDefinition databaseInfo) {}

  @Override
  public void dropDatabase(@NotNull String database) {}

  @Override
  public void createTable(TableDefinition tableInfo) {}

  @Override
  public void dropTable(
      @Nullable String database, @Nullable String schema, @NotNull String table) {}

  @Override
  public void renameTable(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String oldTable,
      @NotNull String newTable) {}

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
