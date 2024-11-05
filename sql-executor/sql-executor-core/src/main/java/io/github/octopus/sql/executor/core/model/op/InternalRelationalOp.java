package io.github.octopus.sql.executor.core.model.op;

import io.github.octopus.sql.executor.core.exception.SqlException;
import io.github.octopus.sql.executor.core.model.schema.ParamValue;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;

public enum InternalRelationalOp implements RelationalOp {
  EQ("=") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  GT(">") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  NOT_GT("!>") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  GE(">=") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  LT("<") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  NOT_LT("!<") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  LE("<=") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  NE("!=") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  LTGT("<>") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  IS_NULL("IS NULL") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return " " + getOp() + " ";
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return " " + getOp() + " ";
    }
  },
  IS_NOT_NULL("IS NOT NULL") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return " " + getOp() + " ";
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return " " + getOp() + " ";
    }
  },
  LIKE("LIKE") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },
  NOT_LIKE("NOT LIKE") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      return toSingleOpSQL(getOp(), paramValues);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      return toSingleOpSQLValue(getOp(), paramValues);
    }
  },

  IN("IN") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 1) {
        throw new SqlException(
            String.format("multi op type is [%s], paramValues cannot be null", getOp()));
      }

      return " " + getOp() + toForeachSQL(this, paramValues[0]);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 1) {
        throw new SqlException(
            String.format("multi op type is [%s], params cannot be null", getOp()));
      }
      ParamValue pv = paramValues[0];
      Object[] values = pv.getValues();
      if (ArrayUtils.isEmpty(values)) {
        throw new SqlException(
            String.format("multi op type is [%s], values cannot be null", getOp()));
      }
      StringBuilder builder = new StringBuilder(" ").append(getOp()).append(" (");
      for (int i = 0; i < values.length; i++) {
        if (pv.getFieldType().isNumeric()) {
          builder.append(values[i]);
        } else {
          builder.append("'").append(values[i]).append("'");
        }
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
    public String toSQL(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 1) {
        throw new SqlException(
            String.format("multi op type is [%s], paramValues cannot be null", getOp()));
      }
      return " " + getOp() + toForeachSQL(this, paramValues[0]);
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 1) {
        throw new SqlException(
            String.format("multi op type is [%s], paramValues cannot be null", getOp()));
      }
      ParamValue pv = paramValues[0];
      Object[] values = pv.getValues();
      if (ArrayUtils.isEmpty(values)) {
        throw new SqlException(
            String.format("multi op type is [%s], values cannot be null", getOp()));
      }
      StringBuilder builder = new StringBuilder(" ").append(getOp()).append(" (");
      for (int i = 0; i < values.length; i++) {
        if (pv.getFieldType().isNumeric()) {
          builder.append(values[i]);
        } else {
          builder.append("'").append(values[i].toString()).append("'");
        }
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
    public String toSQL(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 2) {
        throw new SqlException(
            String.format(
                "multi op type is [%s], paramValues cannot be null or param's size should be equals 2",
                getOp()));
      }
      return " BETWEEN #{params."
          + paramValues[0].getParam()
          + "} AND #{params."
          + paramValues[1].getParam()
          + "} ";
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 2) {
        throw new SqlException(
            String.format(
                "multi op type is [%s], paramValues cannot be null or param's size should be equals 2",
                getOp()));
      }
      ParamValue pvLeft = paramValues[0];
      ParamValue pvRight = paramValues[1];
      Object valueLeft = pvLeft.getValue();
      if (ObjectUtils.isEmpty(valueLeft)) {
        throw new SqlException(
            String.format("multi op type is [%s], left value cannot be blank", getOp()));
      }
      Object valueRight = pvRight.getValue();
      if (ObjectUtils.isEmpty(valueRight)) {
        throw new SqlException(
            String.format("multi op type is [%s], right value cannot be blank", getOp()));
      }

      return " BETWEEN "
          + (pvLeft.getFieldType().isNumeric()
              ? valueLeft
              : String.format("'%s'", valueLeft.toString()))
          + " AND "
          + (pvRight.getFieldType().isNumeric()
              ? valueRight
              : String.format("'%s'", valueRight.toString()))
          + " ";
    }
  },
  NOT_BETWEEN_AND("NOT BETWEEN AND") {
    @Override
    public String toSQL(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 2) {
        throw new SqlException(
            String.format(
                "multi op type is [%s], paramValues cannot be null or param's size should be equals 2",
                getOp()));
      }
      return " NOT BETWEEN #{params."
          + paramValues[0].getParam()
          + "} AND #{params."
          + paramValues[1].getParam()
          + "} ";
    }

    @Override
    public String toSQLValue(ParamValue[] paramValues) {
      if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 2) {
        throw new SqlException(
            String.format(
                "multi op type is [%s], paramValues cannot be null or param's size should be equals 2",
                getOp()));
      }
      ParamValue pvLeft = paramValues[0];
      ParamValue pvRight = paramValues[1];
      Object valueLeft = pvLeft.getValue();
      if (ObjectUtils.isEmpty(valueLeft)) {
        throw new SqlException(
            String.format("multi op type is [%s], left value cannot be blank", getOp()));
      }
      Object valueRight = pvRight.getValue();
      if (ObjectUtils.isEmpty(valueRight)) {
        throw new SqlException(
            String.format("multi op type is [%s], right value cannot be blank", getOp()));
      }
      return " NOT BETWEEN "
          + (pvLeft.getFieldType().isNumeric()
              ? valueLeft
              : String.format("'%s'", valueLeft.toString()))
          + " AND "
          + (pvRight.getFieldType().isNumeric()
              ? valueRight
              : String.format("'%s'", valueRight.toString()))
          + " ";
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

  protected String toSingleOpSQL(String op, ParamValue[] paramValues) {
    if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 1) {
      throw new SqlException(
          String.format(
              "single op type is [%s], param cannot be null or param's size should be equals 1",
              op));
    }
    return String.format(" %s %s ", op, ":" + paramValues[0].getParam());
  }

  protected String toSingleOpSQLValue(String op, ParamValue[] paramValues) {
    if (ArrayUtils.isEmpty(paramValues) && paramValues.length != 1) {
      throw new SqlException(
          String.format(
              "single op type is [%s], params cannot be null or param's size should be equals 1",
              op));
    }
    return String.format(" %s '%s' ", op, paramValues[0].getValue());
  }

  protected String toForeachSQL(RelationalOp op, ParamValue paramValue) {
    StringBuilder builder = new StringBuilder(" (");
    paramValue.multiParamName(op);
    List<String> names = paramValue.getParamIndexes();
    for (int i = 0; i < names.size(); i++) {
      builder.append("#{params.").append(names.get(i)).append("}");
      if (i != names.size() - 1) {
        builder.append(", ");
      }
    }
    return builder.append(") ").toString();
  }
}
