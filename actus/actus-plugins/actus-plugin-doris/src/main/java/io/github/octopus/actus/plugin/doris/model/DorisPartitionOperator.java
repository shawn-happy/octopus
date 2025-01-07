package io.github.octopus.actus.plugin.doris.model;

import io.github.octopus.actus.core.model.schema.PartitionOperator;

public enum DorisPartitionOperator implements PartitionOperator {
  LessThan("LESS THAN"),
  FIXED_RANGE("FIXED RANGE"),
  DATE_MULTI_RANGE("DATE MULTI RANGE"),
  NUMERIC_MULTI_RANGE("NUMERIC MULTI RANGE"),
  ;

  private final String operator;

  DorisPartitionOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String getOperator() {
    return operator;
  }
}
