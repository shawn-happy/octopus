package io.github.octopus.datos.centro.sql.executor.entity;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Privilege {
  private List<String> privileges;
  private String database;
  private String table;

  // for doris
  private String catalog;
  private String user;
  private String host;
  private String role;

  // for mysql
  private List<String> roles;
  private List<User> users;
}
