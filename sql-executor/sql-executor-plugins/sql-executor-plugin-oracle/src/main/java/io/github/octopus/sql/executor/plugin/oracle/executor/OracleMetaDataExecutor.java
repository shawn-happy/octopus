package io.github.octopus.sql.executor.plugin.oracle.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.oracle.dialect.OracleMetaDataStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class OracleMetaDataExecutor extends AbstractMetaDataExecutor {

  public OracleMetaDataExecutor(DataSource dataSource) {
    super(dataSource, OracleMetaDataStatement.getMetaDataStatement());
  }
}
