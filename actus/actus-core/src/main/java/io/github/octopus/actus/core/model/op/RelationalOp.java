package io.github.octopus.actus.core.model.op;

import io.github.octopus.actus.core.model.schema.ParamValue;

public interface RelationalOp {
  String getOp();

  String toSQL(ParamValue[] paramValues);

  String toSQLValue(ParamValue[] paramValues);
}
