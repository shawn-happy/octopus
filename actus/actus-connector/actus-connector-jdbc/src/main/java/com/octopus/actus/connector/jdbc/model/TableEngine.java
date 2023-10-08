package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.DbType;
import com.octopus.actus.connector.jdbc.model.dialect.doris.DorisTableEngine;
import com.octopus.actus.connector.jdbc.model.dialect.mysql.MySQLTableEngine;

public interface TableEngine {
  String getEngine();

  static TableEngine of(DbType dbType, String engine) {
    switch (dbType) {
      case doris:
        return DorisTableEngine.of(engine);
      case mysql:
        return MySQLTableEngine.of(engine);
      default:
        throw new IllegalStateException(String.format("the dbtype [%s] is not supported", dbType));
    }
  }
}
