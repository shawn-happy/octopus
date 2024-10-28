package io.github.octopus.sql.executor.plugin.doris.model;

import io.github.octopus.sql.executor.core.model.schema.PartitionAlgo;
import java.util.Arrays;

public enum DorisPartitionAlgo implements PartitionAlgo {
  Range("RANGE"),
  ;

  private final String algo;

  DorisPartitionAlgo(String algo) {
    this.algo = algo;
  }

  @Override
  public String getAlgo() {
    return algo;
  }

  public static DorisPartitionAlgo of(String algo) {
    return Arrays.stream(values())
        .filter(indexAlgo -> indexAlgo.getAlgo().equalsIgnoreCase(algo))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("The doris Partition Algo [%s] is not Supported", algo)));
  }
}