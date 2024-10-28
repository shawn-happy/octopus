package io.github.octopus.sql.executor.plugin.mysql.model;

import io.github.octopus.sql.executor.core.model.schema.PartitionOperator;

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
