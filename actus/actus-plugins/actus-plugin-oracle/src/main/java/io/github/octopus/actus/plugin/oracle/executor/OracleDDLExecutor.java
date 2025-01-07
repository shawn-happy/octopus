package io.github.octopus.actus.plugin.oracle.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.actus.plugin.oracle.dialect.OracleDDLStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class OracleDDLExecutor extends AbstractDDLExecutor {

  public OracleDDLExecutor(DataSource dataSource) {
    super(dataSource, OracleDDLStatement.getDDLStatement());
  }
}
