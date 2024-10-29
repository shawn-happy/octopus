package io.github.octopus.sql.executor.plugin.oracle.executor;

import io.github.octopus.sql.executor.plugin.api.dao.DDLDao;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.oracle.dao.OracleDDLDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class OracleDDLExecutor extends AbstractDDLExecutor {

  private final Class<? extends DDLDao> dDLDaoClass = OracleDDLDao.class;

  public OracleDDLExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
