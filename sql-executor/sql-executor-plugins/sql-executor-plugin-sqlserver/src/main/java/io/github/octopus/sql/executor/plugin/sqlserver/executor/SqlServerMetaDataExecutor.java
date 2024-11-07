package io.github.octopus.sql.executor.plugin.sqlserver.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.dialect.SqlServerMetaDataStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerMetaDataExecutor extends AbstractMetaDataExecutor {

  public SqlServerMetaDataExecutor(DataSource dataSource) {
    super(dataSource, SqlServerMetaDataStatement.getMetaDataStatement());
  }
}
