package io.github.octopus.sql.executor.plugin.sqlserver.executor;

import io.github.octopus.sql.executor.plugin.api.dao.CurdDao;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.dao.SqlServerCurdDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerCurdExecutor extends AbstractCurdExecutor {

  private final Class<? extends CurdDao> curdDaoClass = SqlServerCurdDao.class;

  public SqlServerCurdExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
