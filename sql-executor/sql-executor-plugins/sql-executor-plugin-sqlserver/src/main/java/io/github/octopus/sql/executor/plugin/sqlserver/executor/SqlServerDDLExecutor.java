package io.github.octopus.sql.executor.plugin.sqlserver.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.dialect.SqlServerDDLStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class SqlServerDDLExecutor extends AbstractDDLExecutor {

  public SqlServerDDLExecutor(DataSource dataSource) {
    super(dataSource, SqlServerDDLStatement.getDDLStatement());
  }
}
