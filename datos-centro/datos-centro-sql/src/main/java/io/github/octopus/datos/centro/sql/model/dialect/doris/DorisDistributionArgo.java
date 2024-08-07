package io.github.octopus.datos.centro.sql.model.dialect.doris;

public enum DorisDistributionArgo implements DistributionAlgo {
  Hash("HASH"),
  Random("RANDOM"),
  ;

  private final String algo;

  DorisDistributionArgo(String algo) {
    this.algo = algo;
  }

  @Override
  public String getAlgo() {
    return algo;
  }
}
