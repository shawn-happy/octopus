package io.github.octopus.datos.centro.model.request.datasource;

import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;

public interface CheckDataSourceRequest<T extends DataSourceConfig> {
  DataSourceType getType();

  T getConfig();
}
