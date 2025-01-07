package io.github.octopus.actus.plugin.oracle.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.actus.plugin.oracle.dialect.OracleMetaDataStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class OracleMetaDataExecutor extends AbstractMetaDataExecutor {

  public OracleMetaDataExecutor(DataSource dataSource) {
    super(dataSource, OracleMetaDataStatement.getMetaDataStatement());
  }
}
