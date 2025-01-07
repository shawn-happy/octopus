package io.github.octopus.actus.plugin.api.executor;

import io.github.octopus.actus.core.model.metadata.ColumnMetaInfo;
import io.github.octopus.actus.core.model.metadata.DatabaseMetaInfo;
import io.github.octopus.actus.core.model.metadata.TableMetaInfo;
import io.github.octopus.actus.plugin.api.dialect.MetaDataStatement;
import java.util.Collections;
import java.util.List;
import javax.sql.DataSource;

public abstract class AbstractMetaDataExecutor extends AbstractExecutor
    implements MetaDataExecutor {

  private final MetaDataStatement metaDataStatement;

  @Override
  public List<DatabaseMetaInfo> getDatabaseInfos() {
    return getProcessor()
        .queryList(metaDataStatement.getDatabaseMetaSql(null), DatabaseMetaInfo.class);
  }

  @Override
  public DatabaseMetaInfo getDatabaseInfo(String database) {
    return getProcessor()
        .queryOne(
            metaDataStatement.getDatabaseMetaSql(Collections.singletonList(database)),
            DatabaseMetaInfo.class);
  }

  @Override
  public List<TableMetaInfo> getTableInfos() {
    return getProcessor()
        .queryList(metaDataStatement.getTableMetaSql(null, null, null), TableMetaInfo.class);
  }

  @Override
  public List<TableMetaInfo> getTableInfos(List<String> database) {
    return getProcessor()
        .queryList(
            metaDataStatement.getTableMetaSql(database.get(0), null, null), TableMetaInfo.class);
  }

  @Override
  public List<TableMetaInfo> getTableInfos(String database, String schemas) {
    return getProcessor()
        .queryList(metaDataStatement.getTableMetaSql(database, schemas, null), TableMetaInfo.class);
  }

  @Override
  public TableMetaInfo getTableInfo(String database, String schemas, String table) {
    return getProcessor()
        .queryOne(
            metaDataStatement.getTableMetaSql(database, schemas, Collections.singletonList(table)),
            TableMetaInfo.class);
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos() {
    return getProcessor()
        .queryList(metaDataStatement.getColumnMetaSql(null, null, null), ColumnMetaInfo.class);
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database, String schemas) {
    return getProcessor()
        .queryList(
            metaDataStatement.getColumnMetaSql(database, schemas, null), ColumnMetaInfo.class);
  }

  @Override
  public List<ColumnMetaInfo> getColumnInfos(String database, String schemas, String table) {
    return getProcessor()
        .queryList(
            metaDataStatement.getColumnMetaSql(database, schemas, table), ColumnMetaInfo.class);
  }

  protected AbstractMetaDataExecutor(DataSource dataSource, MetaDataStatement metaDataStatement) {
    super(dataSource);
    this.metaDataStatement = metaDataStatement;
  }
}
