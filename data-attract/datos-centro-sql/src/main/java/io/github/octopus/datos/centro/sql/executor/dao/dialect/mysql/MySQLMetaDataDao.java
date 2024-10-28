package io.github.octopus.datos.centro.sql.executor.dao.dialect.mysql;

import io.github.octopus.datos.centro.sql.executor.dao.DataWarehouseMetaDataDao;
import io.github.octopus.datos.centro.sql.executor.entity.ColumnMeta;
import io.github.octopus.datos.centro.sql.executor.entity.DatabaseMeta;
import io.github.octopus.datos.centro.sql.executor.entity.TableMeta;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface MySQLMetaDataDao extends DataWarehouseMetaDataDao {

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
