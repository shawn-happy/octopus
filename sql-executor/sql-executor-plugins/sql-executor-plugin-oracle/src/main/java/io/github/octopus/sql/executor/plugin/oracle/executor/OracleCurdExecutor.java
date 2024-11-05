package io.github.octopus.sql.executor.plugin.oracle.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.oracle.dao.OracleCurdDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class OracleCurdExecutor extends AbstractCurdExecutor {

  private final Class<? extends CurdDao> curdDaoClass = OracleCurdDao.class;

  public OracleCurdExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
