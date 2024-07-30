package io.github.octopus.datos.centro.datasource.manager;

import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;

public interface DataSourceManager {

  void connect(DataSourceConfig config);

}
