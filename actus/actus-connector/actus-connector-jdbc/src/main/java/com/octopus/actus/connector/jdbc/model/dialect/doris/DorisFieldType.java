package com.octopus.actus.connector.jdbc.model.dialect.doris;

import com.google.common.collect.ImmutableList;
import com.octopus.actus.connector.jdbc.model.FieldType;
import java.util.Arrays;
import java.util.List;

public enum DorisFieldType implements FieldType {
  TinyInt("TINYINT", "tinyInt类型，范围：-128 to 127"),
  Boolean("BOOLEAN", "BOOLEAN类型，相当于TINYINT(1)，0表示False，非0表示true"),
  SmallInt("SMALLINT", "SmallInt类型，范围：-32768 to 32767"),
  Int("INT", "Int类型，范围：-2147483648 to 2147483647"),
  BigInt("BIGINT", "BigInt类型，范围：-9223372036854775808 to 9223372036854775807"),
  LargeInt("LARGEINT", "LargeInt类型，范围[-2^127 + 1 ~ 2^127 - 1]"),
  Decimal("DECIMAL", "DECIMAL类型，例如：DECIMAL(5,2)，范围：-999.99 to 999.99"),
  Float("FLOAT", "Float类型，单精度"),
  Double("DOUBLE", "Double类型，双精度"),
  Date("DATE", "Date类型，格式：YYYY-MM-DD，范围：'0000-01-01' to '9999-12-31'"),
  DateTime(
      "DATETIME",
      "DateTime类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'0000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'"),
  Varchar("VARCHAR", "Varchar类型，变长字符串"),
  ;
  private final String dataType;
  private final String description;

  private static final List<FieldType> NUMERIC_TYPES =
      ImmutableList.of(TinyInt, SmallInt, Int, BigInt, LargeInt, Decimal, Float, Double);
  private static final List<FieldType> STRING_TYPES = ImmutableList.of(Varchar);
  private static final List<FieldType> DATE_TIME_TYPES = ImmutableList.of(Date, DateTime);

  DorisFieldType(String dataType, String description) {
    this.dataType = dataType;
    this.description = description;
  }

  @Override
  public String getDataType() {
    return dataType;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public boolean isNumeric() {
    return NUMERIC_TYPES.contains(this);
  }

  @Override
  public boolean isString() {
    return STRING_TYPES.contains(this);
  }

  @Override
  public boolean isDateTime() {
    return DATE_TIME_TYPES.contains(this);
  }

  @Override
  public String toString() {
    return String.format("doris datatype: %s, description: %s", dataType, description);
  }

  public static DorisFieldType of(String dataType) {
    if ("bigint unsigned".equalsIgnoreCase(dataType)) {
      return LargeInt;
    }
    return Arrays.stream(values())
        .filter(type -> type.getDataType().equalsIgnoreCase(dataType))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the data type [%s] is not supported with doris.", dataType)));
  }
}
