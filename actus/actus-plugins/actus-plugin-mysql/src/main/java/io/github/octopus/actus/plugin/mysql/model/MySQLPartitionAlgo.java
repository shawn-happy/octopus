package io.github.octopus.actus.plugin.mysql.model;

import io.github.octopus.actus.core.model.schema.PartitionAlgo;
import java.util.Arrays;

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

  public static MySQLPartitionAlgo of(String algo) {
    return Arrays.stream(values())
        .filter(indexAlgo -> indexAlgo.getAlgo().equalsIgnoreCase(algo))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("The mysql Partition Algo [%s] is not Supported", algo)));
  }
}
