package io.github.octopus.datos.centro.sql.model;

import com.alibaba.druid.DbType;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DorisColumnKey;
import io.github.octopus.datos.centro.sql.model.dialect.mysql.MySQLColumnKey;
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
      case starrocks:
        return DorisColumnKey.of(columnKey);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
