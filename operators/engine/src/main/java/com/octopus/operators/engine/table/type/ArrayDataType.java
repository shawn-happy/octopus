package com.octopus.operators.engine.table.type;

public class ArrayDataType implements RowDataType {

  public static final ArrayDataType TINYINT_ARRAY =
      new ArrayDataType(byte[].class, PrimitiveDataType.TINYINT);
  public static final ArrayDataType SMALLINT_ARRAY =
      new ArrayDataType(short[].class, PrimitiveDataType.SMALLINT);
  public static final ArrayDataType INT_ARRAY =
      new ArrayDataType(int[].class, PrimitiveDataType.INT);
  public static final ArrayDataType BIGINT_ARRAY =
      new ArrayDataType(long[].class, PrimitiveDataType.BIGINT);
  public static final ArrayDataType BOOLEAN_ARRAY =
      new ArrayDataType(boolean[].class, PrimitiveDataType.BOOLEAN);
  public static final ArrayDataType FLOAT_ARRAY =
      new ArrayDataType(float[].class, PrimitiveDataType.FLOAT);
  public static final ArrayDataType DOUBLE_ARRAY =
      new ArrayDataType(double[].class, PrimitiveDataType.DOUBLE);
  public static final ArrayDataType STRING_ARRAY =
      new ArrayDataType(String[].class, PrimitiveDataType.STRING);

  private final Class<?> arrayClass;
  private final RowDataType elementClass;

  public ArrayDataType(Class<?> arrayClass, RowDataType elementClass) {
    this.arrayClass = arrayClass;
    this.elementClass = elementClass;
  }

  @Override
  public Class<?> getTypeClass() {
    return arrayClass;
  }

  public RowDataType getElementClass() {
    return elementClass;
  }

  @Override
  public String toString() {
    return String.format("array<%s>", getElementClass().toString());
  }
}
