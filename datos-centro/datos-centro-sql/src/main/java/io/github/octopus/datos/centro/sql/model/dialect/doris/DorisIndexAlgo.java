package io.github.octopus.datos.centro.sql.model.dialect.doris;

import io.github.octopus.datos.centro.sql.model.IndexAlgo;

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
