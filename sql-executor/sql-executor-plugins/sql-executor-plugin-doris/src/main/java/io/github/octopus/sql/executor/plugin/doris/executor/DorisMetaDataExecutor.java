package io.github.octopus.sql.executor.plugin.doris.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.doris.dialect.DorisMetaDataStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class DorisMetaDataExecutor extends AbstractMetaDataExecutor {

  public DorisMetaDataExecutor(DataSource dataSource) {
    super(dataSource, DorisMetaDataStatement.getMetaDataStatement());
  }
}
