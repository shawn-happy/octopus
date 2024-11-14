package io.github.octopus.actus.plugin.sqlserver.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.actus.plugin.sqlserver.dialect.SqlServerDDLStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerDDLExecutor extends AbstractDDLExecutor {

  public SqlServerDDLExecutor(DataSource dataSource) {
    super(dataSource, SqlServerDDLStatement.getDDLStatement());
  }
}
