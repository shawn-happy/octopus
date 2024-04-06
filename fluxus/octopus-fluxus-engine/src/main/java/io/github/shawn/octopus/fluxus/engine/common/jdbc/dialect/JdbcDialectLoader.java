package io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class JdbcDialectLoader {

  private static final List<JdbcDialectProvider> result = new LinkedList<>();

  static {
    ServiceLoader.load(JdbcDialectProvider.class, Thread.currentThread().getContextClassLoader())
        .iterator()
        .forEachRemaining(result::add);
  }

  private JdbcDialectLoader() {}

  /**
   * Loads the unique JDBC Dialect that can handle the given database url.
   *
   * @param url A database URL.
   * @throws IllegalStateException if the loader cannot find exactly one dialect that can
   *     unambiguously process the given database URL.
   * @return The loaded dialect.
   */
  public static JdbcDialect load(String url) {
    List<JdbcDialectProvider> foundFactories = result;
    if (foundFactories.isEmpty()) {
      throw new DataWorkflowException(
          String.format(
              "Could not find any jdbc dialect factories that implement '%s' in the classpath.",
              JdbcDialectProvider.class.getName()));
    }

    final List<JdbcDialectProvider> matchingFactories =
        foundFactories.stream().filter(f -> f.acceptsURL(url)).collect(Collectors.toList());

    if (matchingFactories.isEmpty()) {
      throw new DataWorkflowException(
          String.format(
              "Could not find any jdbc dialect factory that can handle url '%s' that implements '%s' in the classpath.\n\n"
                  + "Available factories are:\n\n"
                  + "%s",
              url,
              JdbcDialectProvider.class.getName(),
              foundFactories
                  .stream()
                  .map(f -> f.getClass().getName())
                  .distinct()
                  .sorted()
                  .collect(Collectors.joining("\n"))));
    }
    if (matchingFactories.size() > 1) {
      throw new DataWorkflowException(
          String.format(
              "Multiple jdbc dialect factories can handle url '%s' that implement '%s' found in the classpath.\n\n"
                  + "Ambiguous factory classes are:\n\n"
                  + "%s",
              url,
              JdbcDialectProvider.class.getName(),
              matchingFactories
                  .stream()
                  .map(f -> f.getClass().getName())
                  .sorted()
                  .collect(Collectors.joining("\n"))));
    }

    return matchingFactories.get(0).createJdbcDialect();
  }
}
