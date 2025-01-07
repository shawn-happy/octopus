package io.github.octopus.actus.plugin.doris.model;

import io.github.octopus.actus.core.model.schema.PartitionAlgo;
import java.util.Arrays;

public enum DorisPartitionAlgo implements PartitionAlgo {
  RANGE("RANGE"),
  LIST("LIST"),
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
