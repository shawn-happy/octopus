package io.github.octopus.sql.executor.plugin.mysql.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.mysql.dialect.MySQLDDLStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLDDLExecutor extends AbstractDDLExecutor {

  public MySQLDDLExecutor(DataSource dataSource) {
    super(dataSource, MySQLDDLStatement.getDDLStatement());
  }
}
