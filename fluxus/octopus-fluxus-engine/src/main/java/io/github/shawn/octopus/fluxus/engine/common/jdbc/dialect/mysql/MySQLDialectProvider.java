package io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.mysql;

import io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.JdbcDialect;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.JdbcDialectProvider;

public class MySQLDialectProvider implements JdbcDialectProvider {
  @Override
  public boolean acceptsURL(String url) {
    return url.startsWith("jdbc:mysql:");
  }

  @Override
  public JdbcDialect createJdbcDialect() {
    return new MySQLDialect();
  }
}
