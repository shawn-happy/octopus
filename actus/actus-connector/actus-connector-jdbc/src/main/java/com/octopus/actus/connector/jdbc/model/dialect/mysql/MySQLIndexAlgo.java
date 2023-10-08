package com.octopus.actus.connector.jdbc.model.dialect.mysql;

import com.octopus.actus.connector.jdbc.model.IndexAlgo;

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
