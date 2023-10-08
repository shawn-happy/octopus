package com.octopus.actus.connector.jdbc.model.dialect.mysql;

import com.octopus.actus.connector.jdbc.model.PartitionOperator;

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
