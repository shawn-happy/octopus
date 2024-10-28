package io.github.octopus.sql.executor.plugin.oracle.executor;

import io.github.octopus.sql.executor.plugin.api.dao.CurdDao;
import io.github.octopus.sql.executor.plugin.api.dao.DDLDao;
import io.github.octopus.sql.executor.plugin.api.dao.MetaDataDao;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;
import io.github.octopus.sql.executor.plugin.api.executor.DDLExecutor;
import javax.sql.DataSource;

public class OracleDDLExecutor extends DDLExecutor {
  public OracleDDLExecutor(String name, DataSource dataSource) {
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
