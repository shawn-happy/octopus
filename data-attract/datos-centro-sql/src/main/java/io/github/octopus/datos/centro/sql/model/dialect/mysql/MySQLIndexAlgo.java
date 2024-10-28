package io.github.octopus.datos.centro.sql.model.dialect.mysql;

import io.github.octopus.datos.centro.sql.model.IndexAlgo;

public enum MySQLIndexAlgo implements IndexAlgo {
  BTREE("BTREE"),
// Innodb不支持Hash索引，只有Memory索引支持，所以暂不提供HASH索引
// HASH("HASH"),
;

  private final String algo;

  MySQLIndexAlgo(String algo) {
    this.algo = algo;
  }

  @Override
  public String getAlgo() {
    return algo;
  }
}
