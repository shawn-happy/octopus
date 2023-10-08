package com.octopus.actus.connector.jdbc.model.dialect.doris;

import com.octopus.actus.connector.jdbc.model.IndexAlgo;

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
}
