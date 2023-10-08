package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisPrivilegeType;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLPrivilegeType;
import org.apache.commons.lang3.StringUtils;

public interface PrivilegeType {
  String getPrivilege();

  static PrivilegeType of(DbType dbType, String privilege) {
    if (StringUtils.isBlank(privilege)) {
      return null;
    }
    switch (dbType) {
      case mysql:
        return MySQLPrivilegeType.of(privilege);
      case doris:
        return DorisPrivilegeType.of(privilege);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
