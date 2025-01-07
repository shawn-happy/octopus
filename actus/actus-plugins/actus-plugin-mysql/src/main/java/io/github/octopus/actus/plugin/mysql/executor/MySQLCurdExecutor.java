package io.github.octopus.actus.plugin.mysql.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.actus.plugin.mysql.dialect.MySQLCurdStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLCurdExecutor extends AbstractCurdExecutor {

  public MySQLCurdExecutor(DataSource dataSource) {
    super(dataSource, MySQLCurdStatement.getCurdStatement());
  }
}
