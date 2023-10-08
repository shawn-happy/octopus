package com.octopus.actus.connector.jdbc.dao;

import com.octopus.actus.connector.jdbc.entity.ColumnMeta;
import com.octopus.actus.connector.jdbc.entity.DatabaseMeta;
import com.octopus.actus.connector.jdbc.entity.TableMeta;
import java.util.List;

public interface MetaDataDao {

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
