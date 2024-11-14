package io.github.octopus.actus.plugin.sqlserver.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.actus.plugin.sqlserver.dialect.SqlServerMetaDataStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerMetaDataExecutor extends AbstractMetaDataExecutor {

  public SqlServerMetaDataExecutor(DataSource dataSource) {
    super(dataSource, SqlServerMetaDataStatement.getMetaDataStatement());
  }
}
