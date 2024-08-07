package io.github.octopus.datos.centro.sql.model;

import com.alibaba.druid.DbType;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DorisPrivilegeType;
import io.github.octopus.datos.centro.sql.model.dialect.mysql.MySQLPrivilegeType;
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
      case starrocks:
        return DorisPrivilegeType.of(privilege);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
