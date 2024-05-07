package com.octopus.operators.connector.flink;

import com.octopus.operators.engine.exception.EngineException;
import com.octopus.operators.engine.table.type.ArrayDataType;
import com.octopus.operators.engine.table.type.DateDataType;
import com.octopus.operators.engine.table.type.DecimalDataType;
import com.octopus.operators.engine.table.type.MapDataType;
import com.octopus.operators.engine.table.type.PrimitiveDataType;
import com.octopus.operators.engine.table.type.RowDataType;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;

public class FlinkDataTypeParser {

  public static TypeInformation<?> parseTypeInformation(RowDataType rowDataType) {
    if (rowDataType == null) {
      return null;
    }
    if (rowDataType instanceof PrimitiveDataType) {
      return parseBasicDataType((PrimitiveDataType) rowDataType);
    } else if (rowDataType instanceof DateDataType) {
      return parseDateDataType((DateDataType) rowDataType);
    } else if (rowDataType instanceof DecimalDataType) {
      return parseDecimalDataType((DecimalDataType) rowDataType);
    } else if (rowDataType instanceof ArrayDataType) {
      return parseArrayDataType((ArrayDataType) rowDataType);
    } else if (rowDataType instanceof MapDataType) {
      return parseMapDataType((MapDataType) rowDataType);
    } else {
      throw new EngineException("unsupported flink data type: " + rowDataType);
    }
  }

  public static TypeInformation<?> parseMapDataType(MapDataType mapDataType) {
    RowDataType keyRowDataType = mapDataType.getKeyRowDataType();
    RowDataType valueRowDataType = mapDataType.getValueRowDataType();
    return new MapTypeInfo<>(
        parseTypeInformation(keyRowDataType), parseTypeInformation(valueRowDataType));
  }

  public static TypeInformation<?> parseArrayDataType(ArrayDataType arrayDataType) {
    RowDataType elementClass = arrayDataType.getElementClass();
    if (elementClass instanceof PrimitiveDataType) {
      PrimitiveDataType primitiveDataType = (PrimitiveDataType) elementClass;
      switch (primitiveDataType) {
        case BOOLEAN:
          return BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
        case TINYINT:
          return BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
        case SMALLINT:
          return BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO;
        case INT:
          return BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
        case BIGINT:
          return BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
        case FLOAT:
          return BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO;
        case DOUBLE:
          return BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
        case STRING:
          return BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
        default:
          throw new EngineException("unsupported flink data type: " + primitiveDataType.name());
      }
    }
    throw new EngineException("unsupported flink data type: " + arrayDataType);
  }

  public static TypeInformation<?> parseDecimalDataType(DecimalDataType decimalDataType) {
    int precision = decimalDataType.getPrecision();
    int scale = decimalDataType.getScale();
    return BigDecimalTypeInfo.of(precision, scale);
  }

  public static TypeInformation<?> parseDateDataType(DateDataType dateDataType) {
    switch (dateDataType) {
      case DATE_TYPE:
        return Types.LOCAL_DATE;
      case DATE_TIME_TYPE:
        return Types.LOCAL_DATE_TIME;
      case TIME_TYPE:
        return Types.LOCAL_TIME;
      default:
        throw new EngineException("unsupported flink data type: " + dateDataType.name());
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
