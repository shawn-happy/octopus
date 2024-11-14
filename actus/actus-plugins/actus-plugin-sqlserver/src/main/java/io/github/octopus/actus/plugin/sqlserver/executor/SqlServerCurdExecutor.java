package io.github.octopus.actus.plugin.sqlserver.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.actus.plugin.sqlserver.dialect.SqlServerCurdStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerCurdExecutor extends AbstractCurdExecutor {

  public SqlServerCurdExecutor(DataSource dataSource) {
    super(dataSource, SqlServerCurdStatement.getCurdStatement());
  }
}
