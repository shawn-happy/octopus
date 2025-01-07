package io.github.octopus.actus.plugin.doris.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.actus.plugin.doris.dialect.DorisCurdStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class DorisCurdExecutor extends AbstractCurdExecutor {

  public DorisCurdExecutor(DataSource dataSource) {
    super(dataSource, DorisCurdStatement.getCurdStatement());
  }
}
