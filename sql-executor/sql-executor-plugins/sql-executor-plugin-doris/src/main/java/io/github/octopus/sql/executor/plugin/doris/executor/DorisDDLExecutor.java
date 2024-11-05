package io.github.octopus.sql.executor.plugin.doris.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.doris.dialect.DorisDDLStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class DorisDDLExecutor extends AbstractDDLExecutor {

  public DorisDDLExecutor(DataSource dataSource) {
    super(dataSource, DorisDDLStatement.getDDLStatement());
  }
}
