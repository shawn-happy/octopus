package com.octopus.operators.engine.table.type;

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

  private final Class<?> primitiveType;

  PrimitiveDataType(Class<?> primitiveType) {
    this.primitiveType = primitiveType;
  }

  @Override
  public Class<?> getTypeClass() {
    return primitiveType;
  }
}
