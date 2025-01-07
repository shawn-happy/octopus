package io.github.octopus.actus.core.model.expression;

import io.github.octopus.actus.core.exception.SqlException;
import io.github.octopus.actus.core.model.op.InternalLogicalOp;
import io.github.octopus.actus.core.model.op.LogicalOp;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;

public class LogicalExpression implements Expression {
  @Getter private final List<Expression> leftExpressions = new ArrayList<>();
  private LogicalOp logicalOp;
  @Getter private final List<Expression> rightExpressions = new ArrayList<>();

  @Override
  public String toSQL() {
    if (CollectionUtils.isEmpty(leftExpressions) || CollectionUtils.isEmpty(rightExpressions)) {
      throw new SqlException(
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
      throw new SqlException(
          "left expression or right expression should not be null when use where statement");
    }
    StringBuilder builder = new StringBuilder(" (");
    leftExpressions.forEach(expression -> builder.append(expression.toSQLValue()));
    builder.append(") ").append(logicalOp.getLogicOp()).append(" (");
    rightExpressions.forEach(expression -> builder.append(expression.toSQLValue()));
    builder.append(") ");
    return builder.toString();
  }

  public Expression and(Expression left, Expression right) {
    if (left == null || right == null) {
      throw new SqlException(
          "left expression or right expression should not be null when use logical operator");
    }
    leftExpressions.add(left);
    logicalOp = InternalLogicalOp.AND;
    rightExpressions.add(right);
    return this;
  }

  public Expression or(Expression left, Expression right) {
    if (left == null || right == null) {
      throw new SqlException(
          "left expression or right expression should not be null when use logical operator");
    }
    leftExpressions.add(left);
    logicalOp = InternalLogicalOp.OR;
    rightExpressions.add(right);
    return this;
  }
}
