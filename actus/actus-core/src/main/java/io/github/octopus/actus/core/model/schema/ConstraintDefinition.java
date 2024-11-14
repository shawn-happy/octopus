package io.github.octopus.actus.core.model.schema;

import io.github.octopus.actus.core.exception.SqlException;
import io.github.octopus.actus.core.model.expression.Expression;

import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConstraintDefinition {
  private String name;
  private List<String> columns;
  private ConstraintType constraintType;
  private String comment;
  // for constraint type is check
  private List<Expression> expressions;

  public List<String> getConstraints() {
    if (constraintType == ConstraintType.CHECK_KEY) {
      if (CollectionUtils.isEmpty(expressions)) {
        throw new SqlException("expression is null when constraint type is check.");
      }
      return expressions.stream().map(Expression::toSQLValue).collect(Collectors.toList());
    }
    return columns;
  }
}
