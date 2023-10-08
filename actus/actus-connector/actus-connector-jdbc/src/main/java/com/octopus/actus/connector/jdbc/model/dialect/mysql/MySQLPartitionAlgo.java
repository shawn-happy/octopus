package com.octopus.actus.connector.jdbc.model.dialect.mysql;

import com.octopus.actus.connector.jdbc.model.PartitionAlgo;

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
