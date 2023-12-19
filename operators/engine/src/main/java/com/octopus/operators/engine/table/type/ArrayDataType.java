package com.octopus.operators.engine.table.type;

public class ArrayDataType implements RowDataType {

  public static final ArrayDataType TINYINT_ARRAY = new ArrayDataType(byte[].class, Byte.TYPE);
  public static final ArrayDataType SMALLINT_ARRAY = new ArrayDataType(short[].class, Short.TYPE);
  public static final ArrayDataType INT_ARRAY = new ArrayDataType(int[].class, Integer.TYPE);
  public static final ArrayDataType BIGINT_ARRAY = new ArrayDataType(long[].class, Long.TYPE);
  public static final ArrayDataType BOOLEAN_ARRAY =
      new ArrayDataType(boolean[].class, Boolean.TYPE);
  public static final ArrayDataType FLOAT_ARRAY = new ArrayDataType(float[].class, Float.TYPE);
  public static final ArrayDataType DOUBLE_ARRAY = new ArrayDataType(double[].class, Double.TYPE);
  public static final ArrayDataType STRING_ARRAY = new ArrayDataType(String[].class, String.class);

  private final Class<?> arrayClass;
  private final Class<?> elementClass;

  public ArrayDataType(Class<?> arrayClass, Class<?> elementClass) {
    this.arrayClass = arrayClass;
    this.elementClass = elementClass;
  }

  @Override
  public Class<?> getTypeClass() {
    return arrayClass;
  }

  public Class<?> getElementClass() {
    return elementClass;
  }

  @Override
  public String toString() {
    return String.format("array<%s>", getElementClass().getSimpleName());
  }
}
