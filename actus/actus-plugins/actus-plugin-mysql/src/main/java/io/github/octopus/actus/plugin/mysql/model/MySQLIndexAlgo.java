package io.github.octopus.actus.plugin.mysql.model;

import io.github.octopus.actus.core.model.schema.IndexAlgo;
import java.util.Arrays;

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

  public static MySQLIndexAlgo of(String algo) {
    return Arrays.stream(values())
        .filter(indexAlgo -> indexAlgo.getAlgo().equalsIgnoreCase(algo))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("The MySQL Index Algo [%s] is not Supported", algo)));
  }
}
