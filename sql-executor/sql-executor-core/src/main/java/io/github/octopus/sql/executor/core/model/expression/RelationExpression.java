package io.github.octopus.sql.executor.core.model.expression;

import io.github.octopus.sql.executor.core.model.op.RelationalOp;
import io.github.octopus.sql.executor.core.model.schema.ParamValue;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RelationExpression implements Expression {

  private String label;
  private RelationalOp relationalOp;
  private ParamValue[] paramValues;

  @Override
  public String toSQL() {
    return label + relationalOp.toSQL(paramValues);
  }

  @Override
  public String toSQLValue() {
    return label + relationalOp.toSQLValue(paramValues);
  }
}
