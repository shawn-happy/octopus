package io.github.octopus.sql.executor.plugin.sqlserver.executor;

import io.github.octopus.sql.executor.plugin.api.dao.CurdDao;
import io.github.octopus.sql.executor.plugin.api.dao.DDLDao;
import io.github.octopus.sql.executor.plugin.api.dao.MetaDataDao;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;
import io.github.octopus.sql.executor.plugin.api.executor.MetaDataExecutor;
import javax.sql.DataSource;

public class SqlServerMetaDataExecutor extends MetaDataExecutor {
  public SqlServerMetaDataExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }

  @Override
  protected JdbcType getJdbcType() {
    return null;
  }

  @Override
  protected Class<? extends CurdDao> getCurdDaoClass() {
    return null;
  }

  @Override
  protected Class<? extends DDLDao> getDDLDaoClass() {
    return null;
  }

  @Override
  protected Class<? extends MetaDataDao> getMetaDataDaoClass() {
    return null;
  }
}
