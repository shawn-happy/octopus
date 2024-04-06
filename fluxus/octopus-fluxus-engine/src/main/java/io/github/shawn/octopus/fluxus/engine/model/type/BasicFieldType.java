package io.github.shawn.octopus.fluxus.engine.model.type;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum BasicFieldType implements DataWorkflowFieldType {
  BOOLEAN(Boolean.class),
  TINYINT(Byte.class),
  SMALLINT(Short.class),
  INT(Integer.class),
  BIGINT(Long.class),
  FLOAT(Float.class),
  DOUBLE(Double.class),
  STRING(String.class),
  ;

  private final Class<?> typeClass;

  BasicFieldType(Class<?> typeClass) {
    this.typeClass = typeClass;
  }

  @Override
  public Class<?> getTypeClass() {
    return typeClass;
  }

  private static final Map<String, BasicFieldType> ALIAS = new HashMap<>();

  static {
    ALIAS.put("long", BIGINT);
    ALIAS.put("integer", INT);
    ALIAS.put("tinyint", TINYINT);
    ALIAS.put("smallint", SMALLINT);
    ALIAS.put("varchar", STRING);
    ALIAS.put("char", STRING);
    ALIAS.put("bool", BOOLEAN);
  }

  public static BasicFieldType of(String type) {
    if (ALIAS.containsKey(type.toLowerCase())) {
      return ALIAS.get(type.toLowerCase());
    }
    return Arrays.stream(values())
        .filter(fieldType -> fieldType.name().equalsIgnoreCase(type))
        .findFirst()
        .orElseThrow(
            () ->
                new DataWorkflowException(
                    String.format("field type [%s] is not supported.", type)));
  }
}
