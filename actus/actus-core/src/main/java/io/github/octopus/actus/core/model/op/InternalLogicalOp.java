package io.github.octopus.actus.core.model.op;

import lombok.Getter;

public enum InternalLogicalOp implements LogicalOp {
  AND("AND"),
  OR("OR"),
  ;

  @Getter private final String logicOp;

  InternalLogicalOp(String logicOp) {
    this.logicOp = logicOp;
  }
}
