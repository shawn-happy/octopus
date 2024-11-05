package io.github.octopus.sql.executor.plugin.doris.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.doris.dao.DorisMetaDataDao;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class DorisAbstractMetaDataExecutor extends AbstractMetaDataExecutor {

  private final Class<? extends MetaDataDao> metaDataDaoClass = DorisMetaDataDao.class;

  public DorisAbstractMetaDataExecutor(String name, DataSource dataSource) {
    super(name, dataSource);
  }
}
