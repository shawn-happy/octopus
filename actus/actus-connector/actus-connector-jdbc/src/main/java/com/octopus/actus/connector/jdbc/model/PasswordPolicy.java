package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisPasswordPolicy;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLPasswordPolicy;
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
      case doris:
        return DorisPasswordPolicy.of(policy);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
