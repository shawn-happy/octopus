package io.github.octopus.datos.centro.model.bo.datasource.jdbc;

import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JdbcDataSourceConfig implements DataSourceConfig {
  private String url;
  private String driverClassName;
  private String username;
  private String password;
  private String database;
}
