package io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect;

public interface JdbcDialectProvider {
  boolean acceptsURL(String url);

  JdbcDialect createJdbcDialect();
}
