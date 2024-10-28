package io.github.octopus.datos.centro.model.bo.datasource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

public class DataSourceTypeProvider {
  private static final Map<String, DataSourceType> DATA_SOURCE_TYPE_MAP = new HashMap<>(16);

  static {
    for (InnerDataSourceType innerDataSourceType : InnerDataSourceType.values()) {
      registerDataSourceType(innerDataSourceType);
    }
    ServiceLoader<DataSourceType> spi = ServiceLoader.load(DataSourceType.class);
    for (DataSourceType dataSourceType : spi) {
      registerDataSourceType(dataSourceType);
    }
  }

  public static void registerDataSourceType(DataSourceType dataSourceType) {
    if (dataSourceType == null) {
      return;
    }
    String identifier = dataSourceType.identifier();
    DATA_SOURCE_TYPE_MAP.putIfAbsent(identifier, dataSourceType);
  }

  public static DataSourceType getDataSourceType(String identifier) {
    return Optional.ofNullable(DATA_SOURCE_TYPE_MAP.get(identifier))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("datasource type [%s] not found.", identifier)));
  }
}
