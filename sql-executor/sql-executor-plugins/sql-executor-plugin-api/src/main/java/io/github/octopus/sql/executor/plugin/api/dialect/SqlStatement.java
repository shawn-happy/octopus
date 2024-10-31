package io.github.octopus.sql.executor.plugin.api.dialect;

public interface SqlStatement {
  JdbcDialect getJdbcDialect();
}

