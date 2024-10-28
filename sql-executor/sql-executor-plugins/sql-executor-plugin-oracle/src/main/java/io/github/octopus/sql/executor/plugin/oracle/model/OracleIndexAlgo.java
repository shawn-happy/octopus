package io.github.octopus.sql.executor.plugin.oracle.model;

import io.github.octopus.sql.executor.core.model.schema.IndexAlgo;
import java.util.Arrays;

public enum OracleIndexAlgo implements IndexAlgo {
  BTREE("UNIQUE"),
  BITMAP("BITMAP"),
  ;

  private final String algo;

  OracleIndexAlgo(String algo) {
    this.algo = algo;
  }

  @Override
  public String getAlgo() {
    return algo;
  }

  public static OracleIndexAlgo of(String algo) {
    return Arrays.stream(values())
        .filter(indexAlgo -> indexAlgo.getAlgo().equalsIgnoreCase(algo))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("The oracle Index Algo [%s] is not Supported", algo)));
  }
}
