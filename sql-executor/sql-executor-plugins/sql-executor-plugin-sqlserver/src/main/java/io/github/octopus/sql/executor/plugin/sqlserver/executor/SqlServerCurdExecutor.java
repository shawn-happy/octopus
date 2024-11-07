package io.github.octopus.sql.executor.plugin.sqlserver.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.dialect.SqlServerCurdStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerCurdExecutor extends AbstractCurdExecutor {

  public SqlServerCurdExecutor(DataSource dataSource) {
    super(dataSource, SqlServerCurdStatement.getCurdStatement());
  }
}
