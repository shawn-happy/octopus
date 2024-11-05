package io.github.octopus.sql.executor.plugin.mysql.executor;

import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.mysql.dialect.MySQLMetaDataStatement;
import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLMetaDataExecutor extends AbstractMetaDataExecutor {

  public MySQLMetaDataExecutor(DataSource dataSource) {
    super(dataSource, MySQLMetaDataStatement.getMetaDataStatement());
  }
}
