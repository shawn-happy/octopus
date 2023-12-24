package com.octopus.operators.engine.table.type;

import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceRow;
import com.octopus.operators.engine.exception.CommonExceptionConstant;
import com.octopus.operators.engine.exception.EngineException;
import com.octopus.operators.engine.table.EngineRow;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

public class RowDataTypeParse {

  public static EngineRow toEngineRow(FakeSourceRow[] fakeSourceRows) {
    String[] fieldNames = new String[fakeSourceRows.length];
    RowDataType[] rowDataTypes = new RowDataType[fakeSourceRows.length];
    for (int i = 0; i < fakeSourceRows.length; i++) {
      fieldNames[i] = fakeSourceRows[i].getFieldName();
      rowDataTypes[i] = parseDataType(fakeSourceRows[i].getFieldType());
    }
    return EngineRow.builder().fieldNames(fieldNames).fieldTypes(rowDataTypes).build();
  }

  public static RowDataType parseDataType(String fieldType) {
    FieldType basicDataType = null;
    try {
      basicDataType = FieldType.of(fieldType);
    } catch (EngineException ignore) {
      // nothing to do
    }
    RowDataType rowDataType = null;
    if (basicDataType == null) {
      rowDataType = parseComplexDataType(fieldType);
    } else {
      rowDataType = parseBasicDataType(basicDataType);
    }
    return rowDataType;
  }

  public static RowDataType parseBasicDataType(@Nonnull String fieldType) {
    return parseBasicDataType(FieldType.of(fieldType));
  }

  public static RowDataType parseBasicDataType(@Nonnull FieldType fieldType) {
    switch (fieldType) {
      case BOOLEAN:
        return PrimitiveDataType.BOOLEAN;
      case TINYINT:
        return PrimitiveDataType.TINYINT;
      case SMALLINT:
        return PrimitiveDataType.SMALLINT;
      case INT:
        return PrimitiveDataType.INT;
      case BIGINT:
        return PrimitiveDataType.BIGINT;
      case FLOAT:
        return PrimitiveDataType.FLOAT;
      case DOUBLE:
        return PrimitiveDataType.DOUBLE;
      case STRING:
        return PrimitiveDataType.STRING;
      case DATE:
        return DateDataType.DATE_TYPE;
      case TIMESTAMP:
      case DATETIME:
        return DateDataType.DATE_TIME_TYPE;
      case TIME:
        return DateDataType.TIME_TYPE;
      case DECIMAL:
      case MAP:
      case ARRAY:
        return null;
      default:
        throw new EngineException(CommonExceptionConstant.unsupportedDataType(fieldType.name()));
    }
  }

  public static RowDataType parseComplexDataType(String fieldType) {
    if (StringUtils.isBlank(fieldType)) {
      throw new EngineException("field type can not be blank");
    }
    if (fieldType.toUpperCase().startsWith(FieldType.ARRAY.name())) {
      return parseArrayDataType(fieldType);
    } else if (fieldType.toUpperCase().startsWith(FieldType.MAP.name())) {
      return parseMapDataType(fieldType);
    } else if (fieldType.toUpperCase().startsWith(FieldType.DECIMAL.name())) {
      return parseDecimalDataType(fieldType);
    }
    throw new EngineException(CommonExceptionConstant.unsupportedDataType(fieldType));
  }

  public static RowDataType parseDecimalDataType(String fieldType) {
    String genericType = getGenericType(fieldType);
    String[] decimalInfos = genericType.split(",");
    if (decimalInfos.length < 2) {
      throw new EngineException(
          String.format(
              "Decimal type should assign precision and scale information. field type: [%s]",
              fieldType));
    }
    int precision = Integer.parseInt(decimalInfos[0].replaceAll("\\D", ""));
    int scale = Integer.parseInt(decimalInfos[1].replaceAll("\\D", ""));
    return new DecimalDataType(precision, scale);
  }

  public static RowDataType parseMapDataType(String fieldType) {
    String genericType = getGenericType(fieldType);
    int index =
        genericType.toUpperCase().startsWith(FieldType.DECIMAL.name())
                || genericType.toUpperCase().startsWith(FieldType.MAP.name())
            ?
            // if map key is decimal, we should find the index of second ','
            StringUtils.ordinalIndexOf(genericType, ",", 2)
            :
            // if map key is not decimal, we should find the index of first ','
            genericType.indexOf(",");
    String keyGenericType = genericType.substring(0, index).trim();
    String valueGenericType = genericType.substring(index + 1).trim();
    return new MapDataType(parseDataType(keyGenericType), parseDataType(valueGenericType));
  }

  public static RowDataType parseArrayDataType(String fieldType) {
    String genericType = getGenericType(fieldType);
    PrimitiveDataType primitiveDataType = null;
    try {
      primitiveDataType = PrimitiveDataType.of(genericType);
    } catch (Exception ignore) {
      throw new EngineException(CommonExceptionConstant.unsupportedDataType(fieldType));
    }
    switch (primitiveDataType) {
      case TINYINT:
        return ArrayDataType.TINYINT_ARRAY;
      case SMALLINT:
        return ArrayDataType.SMALLINT_ARRAY;
      case INT:
        return ArrayDataType.INT_ARRAY;
      case BIGINT:
        return ArrayDataType.BIGINT_ARRAY;
      case FLOAT:
        return ArrayDataType.FLOAT_ARRAY;
      case DOUBLE:
        return ArrayDataType.DOUBLE_ARRAY;
      case BOOLEAN:
        return ArrayDataType.BOOLEAN_ARRAY;
      case STRING:
        return ArrayDataType.STRING_ARRAY;
      default:
        throw new EngineException(CommonExceptionConstant.unsupportedDataType(fieldType));
    }
  }

  private static String getGenericType(String fieldType) {
    return fieldType.substring(fieldType.indexOf("<") + 1, fieldType.lastIndexOf(">")).trim();
  }
}
