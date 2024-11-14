package io.github.octopus.actus.core.jdbc;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

public class DynamicDataSource {
  private static final Map<String, DataSource> DATA_SOURCE_MAP = new ConcurrentHashMap<>();

  public static synchronized void addDataSource(String name, DataSource dataSource) {
    DATA_SOURCE_MAP.putIfAbsent(name, dataSource);
  }

  public static synchronized Optional<DataSource> getDataSource(String name) {
    return Optional.ofNullable(DATA_SOURCE_MAP.get(name));
  }

  public static synchronized DataSource removeDataSource(String name) {
    return DATA_SOURCE_MAP.remove(name);
  }
}
