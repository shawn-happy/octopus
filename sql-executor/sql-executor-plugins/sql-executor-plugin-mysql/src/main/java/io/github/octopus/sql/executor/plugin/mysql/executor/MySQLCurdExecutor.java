package io.github.octopus.sql.executor.plugin.mysql.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.mysql.dialect.MySQLCurdStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLCurdExecutor extends AbstractCurdExecutor {

  public MySQLCurdExecutor(DataSource dataSource) {
    super(dataSource, MySQLCurdStatement.getCurdStatement());
  }
}
