package io.github.octopus.datos.centro.model.request.datasource.edit;

import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;
import io.github.octopus.datos.centro.model.bo.datasource.InnerDataSourceType;
import io.github.octopus.datos.centro.model.bo.datasource.jdbc.JdbcDataSourceConfig;
import io.github.octopus.datos.centro.model.request.datasource.DataSourceRequest;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MySQLDataSourceRequest implements DataSourceRequest<JdbcDataSourceConfig> {
  private DataSourceType type = InnerDataSourceType.MYSQL;
  private String name;
  private String description;
  private JdbcDataSourceConfig config;
}
