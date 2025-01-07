package io.github.octopus.actus.plugin.doris.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.actus.plugin.doris.dialect.DorisMetaDataStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class DorisMetaDataExecutor extends AbstractMetaDataExecutor {

  public DorisMetaDataExecutor(DataSource dataSource) {
    super(dataSource, DorisMetaDataStatement.getMetaDataStatement());
  }
}
