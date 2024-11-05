package io.github.octopus.sql.executor.plugin.api.dialect;

import io.github.octopus.sql.executor.core.exception.SqlException;
import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DialectRegistry {
  private static final Map<String, JdbcDialectFactory> JDBC_DIALECT_FACTORY =
      new ConcurrentHashMap<>();
  private static final Map<String, JdbcDialect> JDBC_DIALECT_MAP = new ConcurrentHashMap<>();

  static {
    try {
      ServiceLoader.load(JdbcDialectFactory.class)
          .iterator()
          .forEachRemaining(
              factory -> {
                JdbcDialect jdbcDialect = factory.create();
                JDBC_DIALECT_FACTORY.putIfAbsent(factory.getDialectName(), factory);
                JDBC_DIALECT_MAP.putIfAbsent(factory.getDialectName(), jdbcDialect);
              });
    } catch (ServiceConfigurationError e) {
      log.error("Could not load service provider for jdbc dialects factory.", e);
      throw new SqlException("Could not load service provider for jdbc dialects factory.", e);
    }
  }

  private DialectRegistry() {}

  public static JdbcDialect getDialect(String dialectName) {
    return Optional.ofNullable(JDBC_DIALECT_MAP.get(dialectName))
        .orElseThrow(() -> new SqlException("Unknown dialect " + dialectName));
  }

  /**
   * Loads the unique JDBC Dialect that can handle the given database url.
   *
   * @throws IllegalStateException if the loader cannot find exactly one dialect that can
   *     unambiguously process the given database URL.
   * @return The loaded dialect.
   */
  public static JdbcDialect load(String dialectName, FieldIdeEnum fieldIde) {
    JdbcDialect jdbcDialect = JDBC_DIALECT_FACTORY.get(dialectName).create(fieldIde);
    JDBC_DIALECT_MAP.put(dialectName, jdbcDialect);
    return jdbcDialect;
  }

  public static void register(String dialectName, JdbcDialectFactory jdbcDialectFactory) {
    JDBC_DIALECT_FACTORY.putIfAbsent(dialectName, jdbcDialectFactory);
  }

  public static List<JdbcDialect> getJdbcDialects() {
    return new ArrayList<>(JDBC_DIALECT_MAP.values());
  }
}
