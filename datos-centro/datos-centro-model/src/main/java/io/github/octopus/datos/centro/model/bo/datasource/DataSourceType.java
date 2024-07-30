package io.github.octopus.datos.centro.model.bo.datasource;

public interface DataSourceType {

  String getPrimaryType();

  String getSecondaryType();

  default String identifier() {
    return String.format("%s_%s", getPrimaryType(), getSecondaryType());
  }
}
