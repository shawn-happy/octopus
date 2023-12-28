package com.octopus.operators.flink.runtime;

import com.octopus.operators.engine.exception.EngineException;
import com.octopus.operators.engine.table.type.PrimitiveDataType;
import com.octopus.operators.engine.table.type.RowDataType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class FlinkDataTypeParser {

  public static TypeInformation<?> parseTypeInformation(RowDataType rowDataType) {
    if (rowDataType == null) {
      return null;
    }
    if (rowDataType instanceof PrimitiveDataType) {
      return parseBasicDataType((PrimitiveDataType) rowDataType);
    } else {
      throw new EngineException("");
    }
  }

  public static TypeInformation<?> parseBasicDataType(PrimitiveDataType primitiveDataType) {
    switch (primitiveDataType) {
      case STRING:
        return Types.STRING;
      case BOOLEAN:
        return Types.BOOLEAN;
      case TINYINT:
        return Types.BYTE;
      case SMALLINT:
        return Types.SHORT;
      case INT:
        return Types.INT;
      case BIGINT:
        return Types.BIG_INT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      default:
        throw new EngineException("unsupported flink data type: " + primitiveDataType.name());
    }
  }
}
