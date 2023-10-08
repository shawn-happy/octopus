package com.octopus.actus.connector.jdbc.dao.dialect.mysql;

import com.octopus.actus.connector.jdbc.dao.MetaDataDao;
import com.octopus.actus.connector.jdbc.entity.ColumnMeta;
import com.octopus.actus.connector.jdbc.entity.DatabaseMeta;
import com.octopus.actus.connector.jdbc.entity.TableMeta;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface MySQLMetaDataDao extends MetaDataDao {

  @Override
  List<DatabaseMeta> getDatabaseMetas();

  @Override
  DatabaseMeta getDatabaseMetaBySchema(@Param("schemaName") String schemaName);

  @Override
  List<TableMeta> getTableMetas();

  @Override
  List<TableMeta> getTableMetasBySchema(@Param("schemaName") String schemaName);

  @Override
  TableMeta getTableMetaBySchemaAndTable(
      @Param("schemaName") String schemaName, @Param("tableName") String tableName);

  @Override
  List<ColumnMeta> getColumnMetas();

  @Override
  List<ColumnMeta> getColumnMetasBySchema(@Param("schemaName") String schemaName);

  @Override
  List<ColumnMeta> getColumnMetasBySchemaAndTable(
      @Param("schemaName") String schemaName, @Param("tableName") String tableName);

  @Override
  ColumnMeta getColumnMetaBySchemaAndTableAndColumn(
      @Param("schemaName") String schemaName,
      @Param("tableName") String tableName,
      @Param("columnName") String columnName);
}
