package io.github.octopus.sql.executor.plugin.doris.executor;

import io.github.octopus.sql.executor.plugin.api.dao.CurdDao;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.doris.dao.DorisCurdDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class DorisCurdExecutor extends AbstractCurdExecutor {

  private final Class<? extends CurdDao> curdDaoClass = DorisCurdDao.class;

  public DorisCurdExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
