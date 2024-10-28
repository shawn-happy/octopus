package io.github.octopus.sql.executor.plugin.api.dao;

import io.github.octopus.sql.executor.core.entity.ColumnMeta;
import io.github.octopus.sql.executor.core.entity.ConstraintKeyMeta;
import io.github.octopus.sql.executor.core.entity.DatabaseMeta;
import io.github.octopus.sql.executor.core.entity.SchemaMeta;
import io.github.octopus.sql.executor.core.entity.TableMeta;
import java.util.List;

public interface MetaDataDao {

  List<DatabaseMeta> getDatabaseMetas();

  DatabaseMeta getDatabaseMetaByDatabase(String database);

  // sqlserver
  default List<SchemaMeta> getSchemaMetas(String database) {
    throw new UnsupportedOperationException("Get Schema MetaData Not supported yet.");
  }

  // sqlserver
  default SchemaMeta getSchemaMetaBySchema(String database, String schema) {
    throw new UnsupportedOperationException("Get Schema MetaData Not supported yet.");
  }

  List<TableMeta> getTableMetas();

  List<TableMeta> getTableMetasByDatabases(List<String> databases);

  List<TableMeta> getTableMetasByDatabaseAndSchema(String database, String schema);

  TableMeta getTableMetasByDatabaseAndSchemaAndTable(String database, String schema, String table);

  List<ColumnMeta> getColumnMetas();

  List<ColumnMeta> getColumnMetasByDatabaseAndSchema(String database, String schema);

  List<ColumnMeta> getColumnMetasByDatabaseAndSchemaAndTable(
      String database, String schema, String table);

  ColumnMeta getColumnMetasByDatabaseAndSchemaAndTableAndColumn(
      String database, String schema, String table, String column);

  List<ConstraintKeyMeta> getConstraintMetas();

  List<ConstraintKeyMeta> getConstraintMetasByDatabaseAndSchema(String database, String schema);

  List<ConstraintKeyMeta> getConstraintMetasByDatabaseAndSchemaAndTable(
      String database, String schema, String table);
}
