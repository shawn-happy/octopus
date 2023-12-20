package com.octopus.operators.engine.table.type;

import com.octopus.operators.engine.exception.CommonExceptionConstant;
import com.octopus.operators.engine.exception.EngineException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum FieldType {
  STRING,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  DATE,
  TIME,
  TIMESTAMP,
  ARRAY,
  MAP,
  DECIMAL,
  ;

  private static final Map<String, FieldType> ROW_DATA_TYPE_MAP = new HashMap<>();

  static {
    ROW_DATA_TYPE_MAP.put("long", FieldType.BIGINT);
    ROW_DATA_TYPE_MAP.put("varchar", FieldType.STRING);
  }

  public static FieldType of(String dataType) {
    FieldType fieldType = ROW_DATA_TYPE_MAP.get(dataType.toLowerCase());
    if (fieldType != null) {
      return fieldType;
    }
    return Arrays.stream(values())
        .filter(primitiveDataType -> primitiveDataType.name().equalsIgnoreCase(dataType))
        .findFirst()
        .orElseThrow(
            () -> new EngineException(CommonExceptionConstant.unsupportedDataType(dataType)));
  }
}
