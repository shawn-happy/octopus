package io.github.octopus.datos.centro.sql.model.dialect.doris;

import io.github.octopus.datos.centro.sql.model.PartitionAlgo;

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
}
