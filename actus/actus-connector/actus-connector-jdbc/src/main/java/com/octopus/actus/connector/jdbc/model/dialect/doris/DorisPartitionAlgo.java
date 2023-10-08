package com.octopus.actus.connector.jdbc.model.dialect.doris;

import com.octopus.actus.connector.jdbc.model.PartitionAlgo;

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
