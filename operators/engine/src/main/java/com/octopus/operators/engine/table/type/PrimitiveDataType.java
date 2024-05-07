package com.octopus.operators.engine.table.type;

import com.octopus.operators.engine.exception.CommonExceptionConstant;
import com.octopus.operators.engine.exception.EngineException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum PrimitiveDataType implements RowDataType {
  BOOLEAN(Boolean.TYPE),
  TINYINT(Byte.TYPE),
  SMALLINT(Short.TYPE),
  INT(Integer.TYPE),
  BIGINT(Long.TYPE),
  FLOAT(Float.TYPE),
  DOUBLE(Double.TYPE),
  STRING(String.class),
  ;

  private static final Map<String, PrimitiveDataType> ROW_DATA_TYPE_MAP = new HashMap<>();

  static {
    ROW_DATA_TYPE_MAP.put("long", PrimitiveDataType.BIGINT);
    ROW_DATA_TYPE_MAP.put("varchar", PrimitiveDataType.STRING);
  }

  private final Class<?> primitiveType;

  PrimitiveDataType(Class<?> primitiveType) {
    this.primitiveType = primitiveType;
  }

  @Override
  public Class<?> getTypeClass() {
    return primitiveType;
  }

  @Override
  public String toString() {
    return this.name().toLowerCase();
  }

  public static PrimitiveDataType of(String dataType) {
    PrimitiveDataType rowDataType = ROW_DATA_TYPE_MAP.get(dataType.toLowerCase());
    if (rowDataType != null) {
      return rowDataType;
    }
    return Arrays.stream(values())
        .filter(primitiveDataType -> primitiveDataType.name().equalsIgnoreCase(dataType))
        .findFirst()
        .orElseThrow(
            () -> new EngineException(CommonExceptionConstant.unsupportedDataType(dataType)));
  }
}
