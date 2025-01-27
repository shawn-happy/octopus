package io.github.octopus.actus.plugin.api.executor;

import io.github.octopus.actus.core.jdbc.JdbcTemplateProcessor;
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
