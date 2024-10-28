package io.github.octopus.sql.executor.plugin.doris.model;

import io.github.octopus.sql.executor.core.model.schema.IndexAlgo;
import java.util.Arrays;

public enum DorisIndexAlgo implements IndexAlgo {
  BITMAP("BITMAP"),
  ;

  private final String algo;

  DorisIndexAlgo(String algo) {
    this.algo = algo;
  }

  @Override
  public String getAlgo() {
    return algo;
  }

  public static DorisIndexAlgo of(String algo) {
    return Arrays.stream(values())
        .filter(indexAlgo -> indexAlgo.getAlgo().equalsIgnoreCase(algo))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("The doris Index Algo [%s] is not Supported", algo)));
  }
}
