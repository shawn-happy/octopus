package io.github.octopus.datos.centro.model.request.datasource;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceConfig;
import io.github.octopus.datos.centro.model.bo.datasource.DataSourceType;
import io.github.octopus.datos.centro.model.request.datasource.edit.MySQLDataSourceRequest;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = MySQLDataSourceRequest.class, name = "jdbc_mysql"),
})
public interface DataSourceRequest<T extends DataSourceConfig> {

  DataSourceType getType();

  String getName();

  String getDescription();

  T getConfig();
}
