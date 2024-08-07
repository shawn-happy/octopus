package io.github.octopus.datos.centro.sql.model;

import io.github.octopus.datos.centro.sql.model.op.RelationalOp;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WhereSimpleExpression implements WhereExpression {

  private String label;
  private RelationalOp relationalOp;
  private ParamValue[] paramValues;

  @Override
  public String toSQL() {
    return label
        + " "
        + relationalOp.toSQL(
            ArrayUtils.isNotEmpty(paramValues)
                ? Arrays.stream(paramValues).map(ParamValue::getParam).toArray(String[]::new)
                : null);
  }

  @Override
  public String toSQLValue() {
    return label + " " + relationalOp.toSQLValue(paramValues);
  }
}
