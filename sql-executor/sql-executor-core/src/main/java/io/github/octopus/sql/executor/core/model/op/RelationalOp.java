package io.github.octopus.sql.executor.core.model.op;

import io.github.octopus.sql.executor.core.model.schema.ParamValue;

public interface RelationalOp {
  String getOp();

  String toSQL(ParamValue[] paramValues);

  String toSQLValue(ParamValue[] paramValues);
}
