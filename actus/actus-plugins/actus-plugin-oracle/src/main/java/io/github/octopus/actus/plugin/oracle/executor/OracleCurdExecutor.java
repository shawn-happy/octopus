package io.github.octopus.actus.plugin.oracle.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.actus.plugin.oracle.dialect.OracleCurdStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class OracleCurdExecutor extends AbstractCurdExecutor {

  public OracleCurdExecutor(DataSource dataSource) {
    super(dataSource, OracleCurdStatement.getCurdStatement());
  }
}
