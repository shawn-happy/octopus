package io.github.shawn.octopus.fluxus.engine.model.type;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.sql.Types;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

public class DataWorkflowFieldTypeParse {

  public static DataWorkflowFieldType parseSqlType(int sqlType, int precision, int scale) {
    switch (sqlType) {
      case Types.BIT:
      case Types.TINYINT:
        return BasicFieldType.TINYINT;
      case Types.SMALLINT:
        return BasicFieldType.SMALLINT;
      case Types.BOOLEAN:
        return BasicFieldType.BOOLEAN;
      case Types.INTEGER:
        return BasicFieldType.INT;
      case Types.BIGINT:
        return BasicFieldType.BIGINT;
      case Types.FLOAT:
        return BasicFieldType.FLOAT;
      case Types.DOUBLE:
        return BasicFieldType.DOUBLE;
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.LONGVARCHAR:
        return BasicFieldType.STRING;
      case Types.DATE:
        return DateTimeFieldType.DATE_TYPE;
      case Types.TIME:
        return DateTimeFieldType.TIME_TYPE;
      case Types.TIMESTAMP:
        return DateTimeFieldType.DATE_TIME_TYPE;
      case Types.NUMERIC:
      case Types.DECIMAL:
        return new DecimalFieldType(precision, scale);
      default:
        throw new DataWorkflowException(
            String.format("the sql type [%d] is not supported", sqlType));
    }
  }

  public static DataWorkflowFieldType parseDataType(String fieldType) {
    FieldType basicDataType = null;
    try {
      basicDataType = FieldType.of(fieldType);
    } catch (DataWorkflowException ignore) {
      // nothing to do
    }
    DataWorkflowFieldType dataWorkflowFieldType = null;
    if (basicDataType == null) {
      dataWorkflowFieldType = parseComplexDataType(fieldType);
    } else {
      dataWorkflowFieldType = parseBasicDataType(basicDataType);
    }
    return dataWorkflowFieldType;
  }

  public static DataWorkflowFieldType parseBasicDataType(@Nonnull String fieldType) {
    return parseBasicDataType(FieldType.of(fieldType));
  }

  public static DataWorkflowFieldType parseBasicDataType(@Nonnull FieldType fieldType) {
    switch (fieldType) {
      case BOOLEAN:
        return BasicFieldType.BOOLEAN;
      case TINYINT:
        return BasicFieldType.TINYINT;
      case SMALLINT:
        return BasicFieldType.SMALLINT;
      case INT:
        return BasicFieldType.INT;
      case BIGINT:
        return BasicFieldType.BIGINT;
      case FLOAT:
        return BasicFieldType.FLOAT;
      case DOUBLE:
        return BasicFieldType.DOUBLE;
      case STRING:
        return BasicFieldType.STRING;
      case DATE:
        return DateTimeFieldType.DATE_TYPE;
      case TIMESTAMP:
      case DATETIME:
        return DateTimeFieldType.DATE_TIME_TYPE;
      case TIME:
        return DateTimeFieldType.TIME_TYPE;
      default:
        throw new DataWorkflowException("");
    }
  }

  public static DataWorkflowFieldType parseComplexDataType(String fieldType) {
    if (StringUtils.isBlank(fieldType)) {
      throw new DataWorkflowException("field type can not be blank");
    }
    if (fieldType.toUpperCase().startsWith("ARRAY")) {
      return parseArrayDataType(fieldType);
    } else if (fieldType.toUpperCase().startsWith("MAP")) {
      return parseMapDataType(fieldType);
    } else if (fieldType.toUpperCase().startsWith("DECIMAL")) {
      return parseDecimalDataType(fieldType);
    }
    throw new DataWorkflowException("");
  }

  public static DataWorkflowFieldType parseDecimalDataType(String fieldType) {
    String genericType = getGenericType(fieldType);
    String[] decimalInfos = genericType.split(",");
    if (decimalInfos.length < 2) {
      throw new DataWorkflowException(
          String.format(
              "Decimal type should assign precision and scale information. field type: [%s]",
              fieldType));
    }
    int precision = Integer.parseInt(decimalInfos[0].replaceAll("\\D", ""));
    int scale = Integer.parseInt(decimalInfos[1].replaceAll("\\D", ""));
    return new DecimalFieldType(precision, scale);
  }

  public static DataWorkflowFieldType parseMapDataType(String fieldType) {
    String genericType = getGenericType(fieldType);
    int index =
        genericType.toUpperCase().startsWith("DECIMAL")
                || genericType.toUpperCase().startsWith("MAP")
            ?
            // if map key is decimal, we should find the index of second ','
            StringUtils.ordinalIndexOf(genericType, ",", 2)
            :
            // if map key is not decimal, we should find the index of first ','
            genericType.indexOf(",");
    String keyGenericType = genericType.substring(0, index).trim();
    String valueGenericType = genericType.substring(index + 1).trim();
    return new MapFieldType(parseDataType(keyGenericType), parseDataType(valueGenericType));
  }

  public static DataWorkflowFieldType parseArrayDataType(String fieldType) {
    String genericType = getGenericType(fieldType);
    BasicFieldType primitiveDataType = null;
    try {
      primitiveDataType = BasicFieldType.of(genericType);
    } catch (Exception ignore) {
      throw new DataWorkflowException("");
    }
    switch (primitiveDataType) {
      case TINYINT:
        return ArrayFieldType.TINYINT_ARRAY;
      case SMALLINT:
        return ArrayFieldType.SMALLINT_ARRAY;
      case INT:
        return ArrayFieldType.INT_ARRAY;
      case BIGINT:
        return ArrayFieldType.BIGINT_ARRAY;
      case FLOAT:
        return ArrayFieldType.FLOAT_ARRAY;
      case DOUBLE:
        return ArrayFieldType.DOUBLE_ARRAY;
      case BOOLEAN:
        return ArrayFieldType.BOOLEAN_ARRAY;
      case STRING:
        return ArrayFieldType.STRING_ARRAY;
      default:
        throw new DataWorkflowException("");
    }
  }

  private static String getGenericType(String fieldType) {
    return fieldType.substring(fieldType.indexOf("<") + 1, fieldType.lastIndexOf(">")).trim();
  }
}
