package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.model.schema.ColumnDefinition;
import io.github.octopus.sql.executor.core.model.schema.DatabaseDefinition;
import io.github.octopus.sql.executor.core.model.schema.IndexDefinition;
import io.github.octopus.sql.executor.core.model.schema.TableDefinition;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface DDLExecutor {
  void createDatabase(DatabaseDefinition databaseInfo);

  void dropDatabase(@NotNull String database);

  void createTable(TableDefinition tableInfo);

  void dropTable(@Nullable String database, @Nullable String schema, @NotNull String table);

  void renameTable(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String oldTable,
      @NotNull String newTable);

  void addTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table, String comment);

  void modifyTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table, String comment);

  void removeTableComment(
      @Nullable String database, @Nullable String schema, @NotNull String table);

  void addColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo);

  void addColumns(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      List<ColumnDefinition> columnInfos);

  void modifyColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition newColumn);

  void renameColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String oldColumn,
      @NotNull String newColumn);

  void dropColumn(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String column);

  void addColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo);

  void modifyColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo);

  void removeColumnComment(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      ColumnDefinition columnInfo);

  void createIndex(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      IndexDefinition indexInfo);

  void createIndexes(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      List<IndexDefinition> indexInfos);

  void dropIndex(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull String index);

  void dropIndexes(
      @Nullable String database,
      @Nullable String schema,
      @NotNull String table,
      @NotNull List<String> indexes);

  void execute(@NotNull String sql);
}
