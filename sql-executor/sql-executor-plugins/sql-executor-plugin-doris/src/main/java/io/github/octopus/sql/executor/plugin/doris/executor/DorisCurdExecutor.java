package io.github.octopus.sql.executor.plugin.doris.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.doris.dialect.DorisCurdStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class DorisCurdExecutor extends AbstractCurdExecutor {

  public DorisCurdExecutor(DataSource dataSource) {
    super(dataSource, DorisCurdStatement.getCurdStatement());
  }
}
