package io.github.shawn.octopus.fluxus.engine.model.type;

import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import lombok.Getter;

@Getter
public enum ArrayFieldType implements DataWorkflowFieldType {
  BOOLEAN_ARRAY(Boolean[].class, BasicFieldType.BOOLEAN),
  TINYINT_ARRAY(Byte[].class, BasicFieldType.TINYINT),
  SMALLINT_ARRAY(Short[].class, BasicFieldType.SMALLINT),
  INT_ARRAY(Integer[].class, BasicFieldType.INT),
  BIGINT_ARRAY(Long[].class, BasicFieldType.BIGINT),
  FLOAT_ARRAY(Float[].class, BasicFieldType.FLOAT),
  DOUBLE_ARRAY(Double[].class, BasicFieldType.DOUBLE),
  STRING_ARRAY(String[].class, BasicFieldType.STRING),
  ;

  private final Class<?> typeClass;
  private final DataWorkflowFieldType elementFieldType;

  ArrayFieldType(Class<?> typeClass, DataWorkflowFieldType elementFieldType) {
    this.typeClass = typeClass;
    this.elementFieldType = elementFieldType;
  }

  @Override
  public String toString() {
    return String.format("array<%s>", getElementFieldType().toString());
  }
}
