package com.octopus.actus.connector.jdbc.model.dialect.mysql;

import com.octopus.actus.connector.jdbc.model.PrivilegeType;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;

public enum MySQLPrivilegeType implements PrivilegeType {
  ALTER("ALTER"),
  CREATE("CREATE"),
  DROP("DROP"),
  DELETE("DELETE"),
  INDEX("INDEX"),
  INSERT("INSERT"),
  UPDATE("UPDATE"),
  SELECT("SELECT"),
  ;

  private final String privilege;

  MySQLPrivilegeType(String privilege) {
    this.privilege = privilege;
  }

  @Override
  public String getPrivilege() {
    return privilege;
  }

  public static MySQLPrivilegeType of(@NotNull String privilege) {
    return Arrays.stream(values())
        .filter(type -> type.getPrivilege().equalsIgnoreCase(privilege))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the privilege [%s] is not supported with mysql", privilege)));
  }
}
