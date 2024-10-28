package io.github.octopus.datos.centro.sql.model.op;

import io.github.octopus.datos.centro.sql.exception.SqlFormatException;
import io.github.octopus.datos.centro.sql.model.ParamValue;
import org.apache.commons.lang3.ArrayUtils;

public enum InternalRelationalOp implements RelationalOp {
  EQ("=") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  GT(">") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  GE(">=") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  LT("<") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  LE("<=") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  NE("!=") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  LTGT("<>") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  IS_NULL("IS NULL") {
    @Override
    public String toSQL(String[] params) {
      return " " + getOp() + " ";
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return " " + getOp() + " ";
    }
  },
  IS_NOT_NULL("IS NOT NULL") {
    @Override
    public String toSQL(String[] params) {
      return " " + getOp() + " ";
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return " " + getOp() + " ";
    }
  },
  LIKE("LIKE") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  NOT_LIKE("NOT LIKE") {
    @Override
    public String toSQL(String[] params) {
      return toSingleOpSQL(getOp(), params);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },

  IN("IN") {
    @Override
    public String toSQL(String[] params) {
      if (ArrayUtils.isEmpty(params)) {
        throw new SqlFormatException(
            String.format("multi op type is [%s], params cannot be null", getOp()));
      }

      return " " + getOp() + toForeachSQL(params[0]);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 1) {
        throw new SqlFormatException(
            String.format("multi op type is [%s], params cannot be null", getOp()));
      }
      ParamValue pv = paramValues[0];
      String[] values = pv.getValues();
      if (ArrayUtils.isEmpty(values)) {
        throw new SqlFormatException(
            String.format("multi op type is [%s], values cannot be null", getOp()));
      }
      StringBuilder builder = new StringBuilder(" ").append(getOp()).append(" (");
      for (int i = 0; i < values.length; i++) {
        builder.append("'").append(values[i]).append("'");
        if (i != values.length - 1) {
          builder.append(",");
        }
      }
      builder.append(")");
      return builder.toString();
    }
  },

  NOT_IN("NOT IN") {
    @Override
    public String toSQL(String[] params) {
      if (ArrayUtils.isEmpty(params)) {
        throw new SqlFormatException(
            String.format("multi op type is [%s], params cannot be null", getOp()));
      }
      return " " + getOp() + toForeachSQL(params[0]);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 1) {
        throw new SqlFormatException(
            String.format("multi op type is [%s], params cannot be null", getOp()));
      }
      ParamValue pv = paramValues[0];
      String[] values = pv.getValues();
      if (ArrayUtils.isEmpty(values)) {
        throw new SqlFormatException(
            String.format("multi op type is [%s], values cannot be null", getOp()));
      }
      StringBuilder builder = new StringBuilder(" ").append(getOp()).append(" (");
      for (int i = 0; i < values.length; i++) {
        builder.append("'").append(values[i]).append("'");
        if (i != values.length - 1) {
          builder.append(",");
        }
      }
      builder.append(")");
      return builder.toString();
    }
  },

  BETWEEN_AND("BETWEEN AND") {
    @Override
    public String toSQL(String[] params) {
      if (ArrayUtils.isEmpty(params) && params.length != 2) {
        throw new SqlFormatException(
            String.format(
                "multi op type is [%s], params cannot be null or param's size should be equals 2",
                getOp()));
      }
      return " BETWEEN #{" + params[0] + "} AND #{" + params[1] + "} ";
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 2) {
        throw new SqlFormatException(
            String.format(
                "multi op type is [%s], params cannot be null or param's size should be equals 2",
                getOp()));
      }
      ParamValue pvLeft = paramValues[0];
      ParamValue pvRight = paramValues[1];
      String[] valueLeft = pvLeft.getValues();
      if (ArrayUtils.isEmpty(valueLeft) && valueLeft.length != 1) {
        throw new SqlFormatException(
            String.format(
                "multi op type is [%s], left value cannot be null or left_value's size should be equals 1",
                getOp()));
      }
      String[] valueRight = pvRight.getValues();
      if (ArrayUtils.isEmpty(valueRight) && valueRight.length != 1) {
        throw new SqlFormatException(
            String.format(
                "multi op type is [%s], right value cannot be null or right_value's size should be equals 1",
                getOp()));
      }
      return " BETWEEN '" + valueLeft[0] + "' AND '" + valueRight[0] + "' ";
    }
  },
  ;

  private final String op;

  InternalRelationalOp(String op) {
    this.op = op;
  }

  @Override
  public String getOp() {
    return op;
  }

  protected String toSingleOpSQL(String op, String[] params) {
    if (ArrayUtils.isEmpty(params) && params.length != 1) {
      throw new SqlFormatException(
          String.format(
              "single op type is [%s], params cannot be null or param's size should be equals 1",
              op));
    }
    return String.format(" %s %s ", op, "#{" + params[0] + "}");
  }

  protected String toSingleOpSQLValue(String op, ParamValue[] paramValues) {
    if (ArrayUtils.isEmpty(paramValues)
        && paramValues.length != 1
        && ArrayUtils.isEmpty(paramValues[0].getValues())
        && paramValues[0].getValues().length != 1) {
      throw new SqlFormatException(
          String.format(
              "single op type is [%s], params cannot be null or param's size should be equals 1",
              op));
    }
    return String.format(" %s '%s' ", op, paramValues[0].getValues()[0]);
  }

  protected String toForeachSQL(String param) {
    return "<foreach collection=\""
        + param
        + "\" item=\"item\" index=\"index\" open=\"(\" close=\")\" separator=\",\">\n"
        + " #{item}\n"
        + "</foreach>";
  }
}
