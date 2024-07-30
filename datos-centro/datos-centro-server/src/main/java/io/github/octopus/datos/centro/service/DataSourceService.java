package io.github.octopus.datos.centro.service;

import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;

public interface DataSourceService {

  <T extends DataSourceConfig> DataSource crateDataSource(
      DataSourceType type, String name, String description, T dataSourceConfig);
}
