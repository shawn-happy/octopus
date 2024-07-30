package io.github.octopus.datos.centro.mapper;

import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import io.github.octopus.datos.centro.entity.DataSourceEntity;
import io.github.octopus.datos.centro.model.bo.datasource.DataSource;
import java.util.Optional;

public class DataSourceMapper {

  public static DataSourceEntity toDsEntity(DataSource ds) {
    return Optional.ofNullable(ds)
        .map(
            datasource -> {
              DataSourceEntity dataSourceEntity = new DataSourceEntity();
              dataSourceEntity.setName(datasource.getName());
              dataSourceEntity.setType(datasource.getType());
              dataSourceEntity.setDescription(dataSourceEntity.getDescription());
              dataSourceEntity.setConfig(null);
              return dataSourceEntity;
            })
        .orElse(null);
  }
}
