package io.github.octopus.datos.centro.sql.executor.service;

import io.github.octopus.datos.centro.sql.model.ColumnMetaInfo;
import io.github.octopus.datos.centro.sql.model.DatabaseInfo;
import io.github.octopus.datos.centro.sql.model.TableMetaInfo;
import java.util.List;

public interface DataWarehouseMetaDataService {

  List<DatabaseInfo> getDatabaseInfos();

  DatabaseInfo getDatabaseInfo(String database);

  List<TableMetaInfo> getTableInfos();

  List<TableMetaInfo> getTableInfos(String database);

  TableMetaInfo getTableInfo(String database, String table);

  List<ColumnMetaInfo> getColumnInfos();

  List<ColumnMetaInfo> getColumnInfos(String database);

  List<ColumnMetaInfo> getColumnInfos(String database, String table);

  ColumnMetaInfo getColumnInfo(String database, String table, String column);
}
