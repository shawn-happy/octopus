package io.github.octopus.sql.executor.plugin.mysql.executor;

import io.github.octopus.sql.executor.core.model.metadata.ColumnMetaInfo;
import io.github.octopus.sql.executor.core.model.metadata.DatabaseMetaInfo;
import io.github.octopus.sql.executor.core.model.metadata.TableMetaInfo;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLMetaDataExecutor extends AbstractMetaDataExecutor {

  public MySQLMetaDataExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }

  @Override
  public List<DatabaseMetaInfo> getDatabaseInfos() {
    return List.of();
  }

  @Override
  public DatabaseMetaInfo getDatabaseInfo(String database) {
    return null;
  }

  @Override
  public List<TableMetaInfo> getTableInfos() {
    return List.of();
  }

  @Override
  public List<TableMetaInfo> getTableInfos(List<String> database) {
    return List.of();
  }

  @Override
  public List<TableMetaInfo> getTableInfos(String database, String schemas) {
    return List.of();
  }

  @Override
  public TableMetaInfo getTableInfo(String database, String schemas, String table) {
    return null;
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos() {
    return List.of();
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database, String schemas) {
    return List.of();
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database, String schemas, String table) {
    return List.of();
  }

  @Override
  public ColumnMetaInfo getColumnInfo(
      String database, String schemas, String table, String column) {
    return null;
  }
}
