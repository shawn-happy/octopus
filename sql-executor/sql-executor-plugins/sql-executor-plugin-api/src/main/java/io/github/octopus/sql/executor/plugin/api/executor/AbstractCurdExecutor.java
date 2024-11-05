package io.github.octopus.sql.executor.plugin.api.executor;

import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractCurdExecutor implements CurdExecutor {

  private final String name;

  protected AbstractCurdExecutor(String name, DataSource dataSource) {
    this.name = name;
  }
}
