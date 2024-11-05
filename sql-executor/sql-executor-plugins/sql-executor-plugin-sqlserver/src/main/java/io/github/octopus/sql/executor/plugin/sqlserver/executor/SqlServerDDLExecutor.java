package io.github.octopus.sql.executor.plugin.sqlserver.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.dao.SqlServerDDLDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerDDLExecutor extends AbstractDDLExecutor {

  private final Class<? extends DDLDao> dDLDaoClass = SqlServerDDLDao.class;

  public SqlServerDDLExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
