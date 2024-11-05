package io.github.octopus.sql.executor.plugin.oracle.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.oracle.dao.OracleMetaDataDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class OracleMetaDataExecutor extends AbstractMetaDataExecutor {

  private final Class<? extends MetaDataDao> metaDataDaoClass = OracleMetaDataDao.class;

  public OracleMetaDataExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
