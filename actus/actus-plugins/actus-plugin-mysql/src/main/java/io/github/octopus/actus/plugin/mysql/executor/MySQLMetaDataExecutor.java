package io.github.octopus.actus.plugin.mysql.executor;

import io.github.octopus.actus.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.actus.plugin.mysql.dialect.MySQLMetaDataStatement;

import javax.sql.DataSource;
import lombok.Getter;

@Getter
public class MySQLMetaDataExecutor extends AbstractMetaDataExecutor {

  public MySQLMetaDataExecutor(DataSource dataSource) {
    super(dataSource, MySQLMetaDataStatement.getMetaDataStatement());
  }
}
