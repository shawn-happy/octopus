package io.github.octopus.actus.plugin.doris.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.actus.plugin.doris.dialect.DorisDDLStatement;

import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class DorisDDLExecutor extends AbstractDDLExecutor {

  public DorisDDLExecutor(DataSource dataSource) {
    super(dataSource, DorisDDLStatement.getDDLStatement());
  }
}
