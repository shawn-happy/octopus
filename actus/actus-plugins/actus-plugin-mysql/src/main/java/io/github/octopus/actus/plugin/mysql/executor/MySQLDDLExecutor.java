package io.github.octopus.actus.plugin.mysql.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.actus.plugin.mysql.dialect.MySQLDDLStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLDDLExecutor extends AbstractDDLExecutor {

  public MySQLDDLExecutor(DataSource dataSource) {
    super(dataSource, MySQLDDLStatement.getDDLStatement());
  }
}
