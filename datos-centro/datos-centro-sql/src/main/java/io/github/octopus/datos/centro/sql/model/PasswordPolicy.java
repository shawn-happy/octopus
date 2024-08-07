package io.github.octopus.datos.centro.sql.model;

import com.alibaba.druid.DbType;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DorisPasswordPolicy;
import io.github.octopus.datos.centro.sql.model.dialect.mysql.MySQLPasswordPolicy;
import org.apache.commons.lang3.StringUtils;

public interface PasswordPolicy {
  String getPolicy();

  static PasswordPolicy of(DbType dbType, String policy) {
    if (StringUtils.isBlank(policy)) {
      return null;
    }
    switch (dbType) {
      case mysql:
        return MySQLPasswordPolicy.of(policy);
      case starrocks:
        return DorisPasswordPolicy.of(policy);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
