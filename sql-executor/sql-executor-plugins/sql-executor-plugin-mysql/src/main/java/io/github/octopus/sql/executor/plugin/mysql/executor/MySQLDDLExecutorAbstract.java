package io.github.octopus.sql.executor.plugin.mysql.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.mysql.dao.MySQLDDLDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLDDLExecutorAbstract extends AbstractDDLExecutor {

  private final Class<? extends DDLDao> dDLDaoClass = MySQLDDLDao.class;

  public MySQLDDLExecutorAbstract(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
