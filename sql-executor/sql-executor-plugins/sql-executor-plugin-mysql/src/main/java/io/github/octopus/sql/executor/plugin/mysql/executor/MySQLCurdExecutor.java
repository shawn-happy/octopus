package io.github.octopus.sql.executor.plugin.mysql.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.mysql.dao.MySQLCurdDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLCurdExecutor extends AbstractCurdExecutor {

  private final Class<? extends CurdDao> curdDaoClass = MySQLCurdDao.class;

  public MySQLCurdExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
