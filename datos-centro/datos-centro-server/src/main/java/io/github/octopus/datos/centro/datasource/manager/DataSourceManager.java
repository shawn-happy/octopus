package io.github.octopus.datos.centro.datasource.manager;

import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;

public interface DataSourceManager<T extends DataSourceConfig> {

  void connect(T config) throws DataCenterServiceException;

}
