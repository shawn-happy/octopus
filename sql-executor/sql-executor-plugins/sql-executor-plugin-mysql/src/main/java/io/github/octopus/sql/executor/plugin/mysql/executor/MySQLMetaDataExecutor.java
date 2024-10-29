package io.github.octopus.sql.executor.plugin.mysql.executor;

import io.github.octopus.sql.executor.plugin.api.dao.MetaDataDao;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.mysql.dao.MySQLMetaDataDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLMetaDataExecutor extends AbstractMetaDataExecutor {

  private final Class<? extends MetaDataDao> metaDataDaoClass = MySQLMetaDataDao.class;

  public MySQLMetaDataExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
