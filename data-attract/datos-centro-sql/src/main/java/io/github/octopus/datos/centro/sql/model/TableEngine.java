package io.github.octopus.datos.centro.sql.model;

import com.alibaba.druid.DbType;
import io.github.octopus.datos.centro.sql.model.dialect.doris.DorisTableEngine;
import io.github.octopus.datos.centro.sql.model.dialect.mysql.MySQLTableEngine;

public interface TableEngine {
  String getEngine();

  static TableEngine of(DbType dbType, String engine) {
    switch (dbType) {
      case starrocks:
        return DorisTableEngine.of(engine);
      case mysql:
        return MySQLTableEngine.of(engine);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
