package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisColumnKey;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLColumnKey;
import org.apache.commons.lang3.StringUtils;

public interface ColumnKey {
  String getKey();

  static ColumnKey of(DbType dbType, String columnKey) {
    if (StringUtils.isBlank(columnKey)) {
      return null;
    }
    switch (dbType) {
      case mysql:
        return MySQLColumnKey.of(columnKey);
      case doris:
        return DorisColumnKey.of(columnKey);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
