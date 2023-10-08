package com.octopus.actus.connector.jdbc.model.dialect.mysql;

import com.google.common.collect.ImmutableList;
import com.octopus.actus.connector.jdbc.model.FieldType;
import java.util.Arrays;
import java.util.List;

public enum MySQLFieldType implements FieldType {
  TinyInt("TINYINT", "tinyInt类型，有符号范围：-128 to 127，无符号范围：0 to 255"),
  Boolean("BOOLEAN", "BOOLEAN类型，相当于TINYINT(1)，0表示False，非0表示true"),
  SmallInt("SMALLINT", "SmallInt类型，有符号范围：-32768 to 32767，无符号范围：0 to 65535"),
  Int("INT", "Int类型，有符号范围：-2147483648 to 2147483647，无符号范围：0 to 4294967295"),
  BigInt(
      "BIGINT",
      "BigInt类型，有符号范围：-9223372036854775808 to 9223372036854775807，无符号范围：0 to 18446744073709551615"),
  Decimal("DECIMAL", "DECIMAL类型，例如：DECIMAL(5,2)，范围：-999.99 to 999.99"),
  Float("FLOAT", "Float类型，单精度"),
  Double("DOUBLE", "Double类型，双精度"),
  Date("DATE", "Date类型，格式：YYYY-MM-DD，范围：'1000-01-01' to '9999-12-31'"),
  DateTime(
      "DATETIME",
      "DateTime类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'"),
  Timestamp(
      "TIMESTAMP",
      "Timestamp类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.499999' UTC"),
  Char("CHAR", "Char类型，定长字符串"),
  Varchar("VARCHAR", "Varchar类型，变长字符串"),
  BLOB("BLOB", "Blob类型"),
  TEXT("TEXT", "TEXT类型，长文本字符串"),
  LongText("LONGTEXT", "LONGTEXT类型，长文本字符串"),
  ;
  private final String dataType;
  private final String description;
  private static final List<FieldType> NUMERIC_TYPES =
      ImmutableList.of(TinyInt, SmallInt, Int, BigInt, Decimal, Float, Double);
  private static final List<FieldType> STRING_TYPES = ImmutableList.of(Varchar);
  private static final List<FieldType> DATE_TIME_TYPES =
      ImmutableList.of(Date, DateTime, Timestamp);

  MySQLFieldType(String dataType, String description) {
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
  public String toString() {
    return String.format("mysql datatype: %s, description: %s", dataType, description);
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

  public static MySQLFieldType of(String dataType) {
    return Arrays.stream(values())
        .filter(type -> type.getDataType().equalsIgnoreCase(dataType))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the data type [%s] is not supported with mysql.", dataType)));
  }
}
