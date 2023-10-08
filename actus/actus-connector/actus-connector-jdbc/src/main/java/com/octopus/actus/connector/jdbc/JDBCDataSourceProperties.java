package com.octopus.actus.connector.jdbc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JDBCDataSourceProperties {
  @NotNull private DbType dbType;
  @NotNull private String name;
  @NotNull private String url;
  private String username;
  private String password;
  @NotNull private String driverClassName;
}
