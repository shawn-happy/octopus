package io.github.octopus.datos.centro.sql.model.dialect.mysql;

import io.github.octopus.datos.centro.sql.model.PartitionOperator;

public enum MySQLPartitionOperator implements PartitionOperator {
  LessThan("LESS THAN"),
  In("IN"),
  ;

  private final String operator;

  MySQLPartitionOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String getOperator() {
    return operator;
  }
}
