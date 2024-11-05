package io.github.octopus.sql.executor.plugin.sqlserver.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.dao.SqlServerMetaDataDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerMetaDataExecutor extends AbstractMetaDataExecutor {

  private final Class<? extends MetaDataDao> metaDataDaoClass = SqlServerMetaDataDao.class;

  public SqlServerMetaDataExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
