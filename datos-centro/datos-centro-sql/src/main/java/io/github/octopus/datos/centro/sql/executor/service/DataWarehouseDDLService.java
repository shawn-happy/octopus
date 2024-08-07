package io.github.octopus.datos.centro.sql.executor.service;

import io.github.octopus.datos.centro.sql.model.ColumnInfo;
import io.github.octopus.datos.centro.sql.model.DatabaseInfo;
import io.github.octopus.datos.centro.sql.model.IndexInfo;
import io.github.octopus.datos.centro.sql.model.TableInfo;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface DataWarehouseDDLService {
  void createDatabase(DatabaseInfo databaseInfo);

  void dropDatabase(@NotNull String database);

  void createTable(TableInfo tableInfo);

  void dropTable(@Nullable String database, @NotNull String table);

  void renameTable(@Nullable String database, @NotNull String oldTable, @NotNull String newTable);

  void addTableComment(@Nullable String database, @NotNull String table, String comment);

  void modifyTableComment(@Nullable String database, @NotNull String table, String comment);

  void removeTableComment(@Nullable String database, @NotNull String table);

  void addColumn(@Nullable String database, @NotNull String table, ColumnInfo columnInfo);

  void addColumns(@Nullable String database, @NotNull String table, List<ColumnInfo> columnInfos);

  void modifyColumn(@Nullable String database, @NotNull String table, ColumnInfo newColumn);

  void renameColumn(
      @Nullable String database,
      @NotNull String table,
      @NotNull String oldColumn,
      @NotNull String newColumn);

  void dropColumn(@Nullable String database, @NotNull String table, @NotNull String column);

  void addColumnComment(@Nullable String database, @NotNull String table, ColumnInfo columnInfo);

  void modifyColumnComment(@Nullable String database, @NotNull String table, ColumnInfo columnInfo);

  void removeColumnComment(@Nullable String database, @NotNull String table, ColumnInfo columnInfo);

  void createIndex(@Nullable String database, @NotNull String table, IndexInfo indexInfo);

  void dropIndex(@Nullable String database, @NotNull String table, @NotNull String index);
}
