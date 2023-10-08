package com.octopus.actus.connector.jdbc.model.op;

import com.octopus.actus.connector.jdbc.model.ParamValue;

public interface RelationalOp {
  String getOp();

  String toSQL(String[] params);

  String toSQLValue(ParamValue[] paramValues);
}
