package io.github.octopus.datos.centro.model.bo.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;

public interface DataSourceType {

  String getPrimaryType();

  String getSecondaryType();

  @JsonCreator
  default String identifier() {
    return String.format("%s_%s", getPrimaryType(), getSecondaryType());
  }
}
