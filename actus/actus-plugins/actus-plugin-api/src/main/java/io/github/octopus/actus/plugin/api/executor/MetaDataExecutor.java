package io.github.octopus.actus.plugin.api.executor;

import io.github.octopus.actus.core.model.metadata.ColumnMetaInfo;
import io.github.octopus.actus.core.model.metadata.DatabaseMetaInfo;
import io.github.octopus.actus.core.model.metadata.TableMetaInfo;
import java.util.List;

public interface MetaDataExecutor {
  List<DatabaseMetaInfo> getDatabaseInfos();

  DatabaseMetaInfo getDatabaseInfo(String database);

  List<TableMetaInfo> getTableInfos();

  List<TableMetaInfo> getTableInfos(List<String> database);

  List<TableMetaInfo> getTableInfos(String database, String schemas);

  TableMetaInfo getTableInfo(String database, String schemas, String table);

  List<ColumnMetaInfo> getColumnInfos();

  List<ColumnMetaInfo> getColumnInfos(String database, String schemas);

  List<ColumnMetaInfo> getColumnInfos(String database, String schemas, String table);

  ColumnMetaInfo getColumnInfo(String database, String schemas, String table, String column);
}
