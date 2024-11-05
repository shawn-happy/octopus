package io.github.octopus.sql.executor.plugin.api.executor;

import javax.sql.DataSource;

public abstract class AbstractMetaDataExecutor implements MetaDataExecutor {

  private final String name;

  protected AbstractMetaDataExecutor(String name, DataSource dataSource) {
    this.name = name;
  }
}
