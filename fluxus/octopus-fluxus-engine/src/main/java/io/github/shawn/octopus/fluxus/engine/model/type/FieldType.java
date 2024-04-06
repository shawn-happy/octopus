package io.github.shawn.octopus.fluxus.engine.model.type;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum FieldType {
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  BOOLEAN,
  FLOAT,
  DOUBLE,
  STRING,
  DATE,
  DATETIME,
  TIMESTAMP,
  TIME,
  ;

  private static final Map<String, FieldType> ALIAS = new HashMap<>();

  static {
    ALIAS.put("long", BIGINT);
    ALIAS.put("integer", INT);
    ALIAS.put("tinyint", TINYINT);
    ALIAS.put("smallint", SMALLINT);
    ALIAS.put("varchar", STRING);
    ALIAS.put("char", STRING);
    ALIAS.put("bool", BOOLEAN);
  }

  public static FieldType of(String type) {
    FieldType fieldType = ALIAS.get(type.toLowerCase());
    if (fieldType != null) {
      return fieldType;
    }
    return Arrays.stream(values())
        .filter(e -> e.name().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(
            () ->
                new DataWorkflowException(
                    String.format("field type [%s] is not supported.", type)));
  }
}
