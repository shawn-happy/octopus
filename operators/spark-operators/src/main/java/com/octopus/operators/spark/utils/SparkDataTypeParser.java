package com.octopus.operators.spark.utils;

import com.octopus.operators.engine.exception.CommonExceptionConstant;
import com.octopus.operators.engine.exception.EngineException;
import com.octopus.operators.engine.table.type.ArrayDataType;
import com.octopus.operators.engine.table.type.DateDataType;
import com.octopus.operators.engine.table.type.DecimalDataType;
import com.octopus.operators.engine.table.type.MapDataType;
import com.octopus.operators.engine.table.type.PrimitiveDataType;
import com.octopus.operators.engine.table.type.RowDataType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class SparkDataTypeParser {

  public static DataType toDataType(RowDataType rowDataType) {
    if (rowDataType == null) {
      return null;
    }
    if (rowDataType instanceof PrimitiveDataType) {
      return parsePrimitiveDataType((PrimitiveDataType) rowDataType);
    } else if (rowDataType instanceof DateDataType) {
      return parseDateDataType((DateDataType) rowDataType);
    } else if (rowDataType instanceof DecimalDataType) {
      return parseDecimalDataType((DecimalDataType) rowDataType);
    } else if (rowDataType instanceof ArrayDataType) {
      return parseArrayDataType((ArrayDataType) rowDataType);
    } else if (rowDataType instanceof MapDataType) {
      return parseMapDataType((MapDataType) rowDataType);
    }
    throw new EngineException(CommonExceptionConstant.unsupportedDataType(rowDataType.toString()));
  }

  public static DataType parsePrimitiveDataType(PrimitiveDataType primitiveDataType) {
    if (primitiveDataType == null) {
      return null;
    }
    switch (primitiveDataType) {
      case TINYINT:
        return DataTypes.ByteType;
      case SMALLINT:
        return DataTypes.ShortType;
      case INT:
        return DataTypes.IntegerType;
      case BIGINT:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case STRING:
        return DataTypes.StringType;
      default:
        throw new EngineException(
            CommonExceptionConstant.unsupportedDataType(primitiveDataType.name()));
    }
  }

  public static DataType parseDateDataType(DateDataType dateDataType) {
    if (dateDataType == null) {
      return null;
    }
    switch (dateDataType) {
      case DATE_TYPE:
        return DataTypes.DateType;
      case DATE_TIME_TYPE:
      case TIME_TYPE:
        return DataTypes.TimestampType;
      default:
        throw new EngineException(CommonExceptionConstant.unsupportedDataType(dateDataType.name()));
    }
  }

  public static DataType parseDecimalDataType(DecimalDataType decimalDataType) {
    if (decimalDataType == null) {
      return null;
    }
    return DataTypes.createDecimalType(decimalDataType.getPrecision(), decimalDataType.getScale());
  }

  public static DataType parseArrayDataType(ArrayDataType arrayDataType) {
    if (arrayDataType == null) {
      return null;
    }
    RowDataType elementClass = arrayDataType.getElementClass();
    return DataTypes.createArrayType(toDataType(elementClass));
  }

  public static DataType parseMapDataType(MapDataType mapDataType) {
    if (mapDataType == null) {
      return null;
    }
    RowDataType keyRowDataType = mapDataType.getKeyRowDataType();
    RowDataType valueRowDataType = mapDataType.getValueRowDataType();
    return DataTypes.createMapType(toDataType(keyRowDataType), toDataType(valueRowDataType));
  }
}
