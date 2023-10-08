package com.octopus.actus.connector.jdbc.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PrivilegeInfo {
  private List<PrivilegeType> privileges;
  private String database;
  private String table;

  // for doris
  private String catalog;

  // for mysql
  private List<String> roles;
  private List<UserInfo> userInfos;
}
