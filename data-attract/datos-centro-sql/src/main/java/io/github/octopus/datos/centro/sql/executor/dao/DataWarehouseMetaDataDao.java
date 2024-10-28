package io.github.octopus.datos.centro.sql.executor.dao;

import io.github.octopus.datos.centro.sql.executor.entity.ColumnMeta;
import io.github.octopus.datos.centro.sql.executor.entity.DatabaseMeta;
import io.github.octopus.datos.centro.sql.executor.entity.TableMeta;
import java.util.List;

public interface DataWarehouseMetaDataDao {

  List<DatabaseMeta> getDatabaseMetas();

  DatabaseMeta getDatabaseMetaBySchema(String schemaName);

  List<TableMeta> getTableMetas();

  List<TableMeta> getTableMetasBySchema(String schemaName);

  TableMeta getTableMetaBySchemaAndTable(String schemaName, String tableName);

  List<ColumnMeta> getColumnMetas();

  List<ColumnMeta> getColumnMetasBySchema(String schemaName);

  List<ColumnMeta> getColumnMetasBySchemaAndTable(String schemaName, String tableName);

  ColumnMeta getColumnMetaBySchemaAndTableAndColumn(
      String schemaName, String tableName, String columnName);
}
