package io.github.octopus.sql.executor.plugin.api.executor;

import io.github.octopus.sql.executor.core.jdbc.JdbcTemplateProcessor;
import javax.sql.DataSource;

public abstract class AbstractExecutor {

  private final JdbcTemplateProcessor processor;

  protected AbstractExecutor(DataSource dataSource) {
    this.processor = new JdbcTemplateProcessor(dataSource);
  }

  public JdbcTemplateProcessor getProcessor() {
    return processor;
  }
}
