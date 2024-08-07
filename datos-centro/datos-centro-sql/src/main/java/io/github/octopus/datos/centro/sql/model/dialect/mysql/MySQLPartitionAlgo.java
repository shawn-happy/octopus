package io.github.octopus.datos.centro.sql.model.dialect.mysql;

import io.github.octopus.datos.centro.sql.model.PartitionAlgo;

public enum MySQLPartitionAlgo implements PartitionAlgo {
  Hash("HASH"),
  Linear_Hash("LINEAR HASH"),
  Key("KEY"),
  Linear_Key("LINEAR KEY"),
  Range("RANGE"),
  List("List"),
  ;

  private final String algo;

  MySQLPartitionAlgo(String algo) {
    this.algo = algo;
  }

  @Override
  public String getAlgo() {
    return algo;
  }
}
