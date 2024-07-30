package io.github.octopus.datos.centro.datasource.factory;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class DataSourceFactoryProvider {

  private static final Map<String, DataSourceFactory> DATA_SOURCE_FACTORY_MAP =
      new ConcurrentHashMap<>(16);

  static {
    ServiceLoader<DataSourceFactory> spi = ServiceLoader.load(DataSourceFactory.class);
    for (DataSourceFactory dataSourceFactory : spi) {
      registryDataSourceFactory(dataSourceFactory);
    }
  }

  public static DataSourceFactory getDataSourceFactory(String type) {
    return Optional.ofNullable(DATA_SOURCE_FACTORY_MAP.get(type))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("The datasource type [%s] is not found.", type)));
  }

  public static void registryDataSourceFactory(DataSourceFactory dataSourceFactory) {
    if (dataSourceFactory == null) {
      return;
    }
    String dataSourceType = dataSourceFactory.factoryIdentifier();
    DATA_SOURCE_FACTORY_MAP.putIfAbsent(dataSourceType, dataSourceFactory);
  }
}
