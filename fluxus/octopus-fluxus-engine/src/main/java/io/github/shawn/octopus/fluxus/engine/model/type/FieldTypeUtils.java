package io.github.shawn.octopus.fluxus.engine.model.type;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.sql.Types;

public class FieldTypeUtils {
  public static int getSqlType(DataWorkflowFieldType fieldType) {
    if (fieldType == null) {
      return Types.NULL;
    }
    if (fieldType instanceof BasicFieldType) {
      BasicFieldType basicFieldType = (BasicFieldType) fieldType;
      switch (basicFieldType) {
        case BOOLEAN:
          return Types.BOOLEAN;
        case TINYINT:
          return Types.TINYINT;
        case SMALLINT:
          return Types.SMALLINT;
        case INT:
          return Types.INTEGER;
        case BIGINT:
          return Types.BIGINT;
        case STRING:
          return Types.VARCHAR;
        case FLOAT:
          return Types.FLOAT;
        case DOUBLE:
          return Types.DOUBLE;
        default:
          throw new DataWorkflowException("unsupported field type");
      }
    } else if (fieldType instanceof DateTimeFieldType) {
      DateTimeFieldType dateTimeFieldType = (DateTimeFieldType) fieldType;
      switch (dateTimeFieldType) {
        case TIME_TYPE:
          return Types.TIME;
        case DATE_TYPE:
          return Types.DATE;
        case DATE_TIME_TYPE:
          return Types.TIMESTAMP;
        default:
          throw new DataWorkflowException("unsupported field type");
      }
    } else if (fieldType instanceof DecimalFieldType) {
      return Types.DECIMAL;
    }
    throw new DataWorkflowException("unsupported sql type");
  }
}
