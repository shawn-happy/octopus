package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.exception.SqlFormatException;
import com.octopus.actus.connector.jdbc.model.op.InternalLogicalOp;
import com.octopus.actus.connector.jdbc.model.op.LogicalOp;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;

public class WhereLogicalExpression implements WhereExpression {
  @Getter private final List<WhereExpression> leftExpressions = new ArrayList<>();
  private LogicalOp logicalOp;
  @Getter private final List<WhereExpression> rightExpressions = new ArrayList<>();

  @Override
  public String toSQL() {
    if (CollectionUtils.isEmpty(leftExpressions) || CollectionUtils.isEmpty(rightExpressions)) {
      throw new SqlFormatException(
          "left expression or right expression should not be null when use where statement");
    }
    StringBuilder builder = new StringBuilder(" (");
    leftExpressions.forEach(expression -> builder.append(expression.toSQL()));
    builder.append(") ").append(logicalOp.getLogicOp()).append(" (");
    rightExpressions.forEach(expression -> builder.append(expression.toSQL()));
    builder.append(") ");
    return builder.toString();
  }

  @Override
  public String toSQLValue() {
    if (CollectionUtils.isEmpty(leftExpressions) || CollectionUtils.isEmpty(rightExpressions)) {
      throw new SqlFormatException(
          "left expression or right expression should not be null when use where statement");
    }
    StringBuilder builder = new StringBuilder(" (");
    leftExpressions.forEach(expression -> builder.append(expression.toSQLValue()));
    builder.append(") ").append(logicalOp.getLogicOp()).append(" (");
    rightExpressions.forEach(expression -> builder.append(expression.toSQLValue()));
    builder.append(") ");
    return builder.toString();
  }

  public WhereExpression and(WhereExpression left, WhereExpression right) {
    if (left == null || right == null) {
      throw new SqlFormatException(
          "left expression or right expression should not be null when use logical operator");
    }
    leftExpressions.add(left);
    logicalOp = InternalLogicalOp.AND;
    rightExpressions.add(right);
    return this;
  }

  public WhereExpression or(WhereExpression left, WhereExpression right) {
    if (left == null || right == null) {
      throw new SqlFormatException(
          "left expression or right expression should not be null when use logical operator");
    }
    leftExpressions.add(left);
    logicalOp = InternalLogicalOp.OR;
    rightExpressions.add(right);
    return this;
  }
}
