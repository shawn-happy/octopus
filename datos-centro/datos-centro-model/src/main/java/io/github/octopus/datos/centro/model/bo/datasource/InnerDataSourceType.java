package io.github.octopus.datos.centro.model.bo.datasource;

import java.util.Arrays;
import lombok.Getter;

@Getter
public enum InnerDataSourceType implements DataSourceType{

  MYSQL("jdbc", "mysql"),
  ;

  private final String primaryType;
  private final String secondaryType;

  InnerDataSourceType(String primaryType, String secondaryType) {
    this.primaryType = primaryType;
    this.secondaryType = secondaryType;
  }

  public static DataSourceType of(String type) {
    return Arrays.stream(values())
        .filter(dsType -> dsType.name().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("Unsupported DataSource Type: [%s]", type)));
  }
}
