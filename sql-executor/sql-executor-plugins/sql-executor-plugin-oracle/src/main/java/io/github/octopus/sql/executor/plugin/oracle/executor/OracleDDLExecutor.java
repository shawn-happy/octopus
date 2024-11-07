package io.github.octopus.sql.executor.plugin.oracle.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.oracle.dialect.OracleDDLStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class OracleDDLExecutor extends AbstractDDLExecutor {

  public OracleDDLExecutor(DataSource dataSource) {
    super(dataSource, OracleDDLStatement.getDDLStatement());
  }
}
