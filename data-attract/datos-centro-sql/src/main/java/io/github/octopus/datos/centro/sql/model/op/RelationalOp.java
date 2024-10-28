package io.github.octopus.datos.centro.sql.model.op;

import io.github.octopus.datos.centro.sql.model.ParamValue;

public interface RelationalOp {
  String getOp();

  String toSQL(String[] params);

  String toSQLValue(ParamValue[] paramValues);
}
