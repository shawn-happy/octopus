package io.github.octopus.sql.executor.plugin.mysql.model;

import com.google.common.collect.ImmutableList;
import io.github.octopus.sql.executor.core.model.schema.FieldType;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public enum MySQLFieldType implements FieldType {
  BIT(
      "BIT", Types.BIT,
      FieldDescriptor.builder()
          .chName("位值类型")
          .description("bit类型，用于存储位值")
          .maxPrecision(64)
          .minPrecision(1)
          .build()),

  TINYINT(
      "TINYINT",Types.TINYINT,
      FieldDescriptor.builder()
          .chName("极小整数型")
          .description("tinyInt类型，有符号范围：-128 to 127，无符号范围：0 to 255")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  BOOLEAN(
      "BOOLEAN",Types.BOOLEAN,
      FieldDescriptor.builder()
          .chName("布尔类型")
          .description("BOOLEAN类型，相当于TINYINT(1)，0表示False，非0表示true")
          .build()),
  SMALLINT(
      "SMALLINT", Types.SMALLINT,
      FieldDescriptor.builder()
          .chName("短整数型")
          .description("SmallInt类型，有符号范围：-32768 to 32767，无符号范围：0 to 65535")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  INT(
      "INT", Types.INTEGER,
      FieldDescriptor.builder()
          .chName("整数类型")
          .description("Int类型，有符号范围：-2147483648 to 2147483647，无符号范围：0 to 4294967295")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  MEDIUMINT(
      "MEDIUMINT", Types.INTEGER,
      FieldDescriptor.builder()
          .chName("较小整数类型")
          .description("MEDIUMINT，有符号范围：-8388608 to 8388607，无符号范围：0 to 16777215")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  BIGINT(
      "BIGINT",Types.BIGINT,
      FieldDescriptor.builder()
          .chName("长整数型")
          .description(
              "BigInt类型，有符号范围：-9223372036854775808 to 9223372036854775807，无符号范围：0 to 18446744073709551615")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  DECIMAL(
      "DECIMAL", Types.DECIMAL,
      FieldDescriptor.builder()
          .chName("数值类型")
          .description("DECIMAL类型，例如：DECIMAL(5,2)，范围：-999.99 to 999.99")
          .maxPrecision(65)
          .minPrecision(0)
          .minScale(0)
          .maxScale(30)
          .build()),
  FLOAT(
      "FLOAT", Types.FLOAT,
      FieldDescriptor.builder()
          .chName("单精度浮点类型")
          .description("Float类型，单精度")
          .maxPrecision(255)
          .minPrecision(0)
          .minScale(0)
          .maxScale(30)
          .build()),
  DOUBLE(
      "DOUBLE", Types.DOUBLE,
      FieldDescriptor.builder()
          .chName("双精度浮点类型")
          .description("Double类型，双精度")
          .maxPrecision(255)
          .minPrecision(0)
          .minScale(0)
          .maxScale(30)
          .build()),
  DATE(
      "DATE", Types.DATE,
      FieldDescriptor.builder()
          .chName("日期类型")
          .description("Date类型，格式：YYYY-MM-DD，范围：'1000-01-01' to '9999-12-31'")
          .build()),
  DATETIME(
      "DATETIME", Types.TIMESTAMP,
      FieldDescriptor.builder()
          .chName("日期时间类型")
          .description(
              "DateTime类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  TIME(
      "TIME", Types.TIME,
      FieldDescriptor.builder()
          .chName("时间类型")
          .description(
              "Time类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'-838:59:59.000000' to '838:59:59.000000'")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  TIMESTAMP(
      "TIMESTAMP",Types.TIMESTAMP,
      FieldDescriptor.builder()
          .chName("时间戳类型")
          .description(
              "Timestamp类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.499999' UTC")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  VARCHAR(
      "VARCHAR",Types.VARCHAR,
      FieldDescriptor.builder()
          .chName("字符串类型")
          .description("Varchar类型，变长字符串")
          .maxPrecision(65535)
          .minPrecision(0)
          .build()),
  CHAR(
      "CHAR",Types.CHAR,
      FieldDescriptor.builder()
          .chName("字符类型")
          .description("字符类型，固定长度")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  TINYTEXT(
      "TINYTEXT", Types., FieldDescriptor.builder().chName("短文本类型").description("TEXT类型，长文本字符串").build()),
  TEXT("TEXT", FieldDescriptor.builder().chName("文本类型").description("TEXT类型，长文本字符串").build()),
  MEDIUMTEXT(
      "MEDIUMTEXT",
      FieldDescriptor.builder().chName("中文本类型").description("MEDIUMTEXT类型，存储大小比TEXT类型较大").build()),
  LongText(
      "LONGTEXT",
      FieldDescriptor.builder().chName("长文本类型").description("LONGTEXT类型，长文本字符串").build()),

  TINYBLOB(
      "TINYBLOB",
      FieldDescriptor.builder().chName("小二进制类型").description("即二进制大对象，存储大小比BLOB略小").build()),
  BLOB("BLOB", FieldDescriptor.builder().chName("二进制类型").description("即二进制大对象，用于存储二进制数据").build()),
  MEDIUMBLOB(
      "MEDIUMBLOB",
      FieldDescriptor.builder().chName("中二进制类型").description("即二进制大对象，存储大小比BLOB略大").build()),
  LONG_BLOB(
      "LONGBLOB", FieldDescriptor.builder().chName("大二进制类型").description("存储更大容量二进制数据的类型").build()),

  JSON("JSON", FieldDescriptor.builder().chName("JSON类型").description("JSON类型").build()),
  BINARY("BINARY", FieldDescriptor.builder().chName("BINARY类型").description("与Char类型相似").build()),
  VARBINARY(
      "VARBINARY",
      FieldDescriptor.builder().chName("VARBINARY类型").description("与VarChar类型相似").build()),
  YEAR("YEAR", FieldDescriptor.builder().chName("年类型").description("年类型，格式为yyyy，例如2024").build()),
  ENUM("ENUM", FieldDescriptor.builder().chName("枚举类型").description("枚举类型").build()),
  ;
  private final String dataType;
  private final int sqlType;
  private final FieldDescriptor fieldDescriptor;

  private static final List<FieldType> NUMERIC_TYPES =
      ImmutableList.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, FLOAT, DOUBLE);
  private static final List<FieldType> STRING_TYPES = ImmutableList.of(VARCHAR);
  private static final List<FieldType> DATE_TIME_TYPES =
      ImmutableList.of(DATE, DATETIME, TIMESTAMP);

  MySQLFieldType(String dataType, int sqlType, FieldDescriptor fieldDescriptor) {
    this.dataType = dataType;
    this.sqlType = sqlType;
    this.fieldDescriptor = fieldDescriptor;
  }

  @Override
  public String getDataType() {
    return dataType;
  }

  @Override
  public String getChName() {
    return fieldDescriptor.getChName();
  }

  @Override
  public String getDescription() {
    return fieldDescriptor.getDescription();
  }

  @Override
  public int getSqlType(){
    return sqlType;
  }

  @Override
  public String toString() {
    return String.format("mysql datatype: %s, %s", dataType, fieldDescriptor.toString());
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
  public Integer getMinPrecision() {
    return fieldDescriptor.getMinPrecision();
  }

  @Override
  public Integer getMaxPrecision() {
    return fieldDescriptor.getMaxPrecision();
  }

  @Override
  public Integer getMinScale() {
    return fieldDescriptor.getMinScale();
  }

  @Override
  public Integer getMaxScale() {
    return fieldDescriptor.getMaxScale();
  }

  @Override
  public boolean hasPrecision() {
    return fieldDescriptor.getMinPrecision() != null && fieldDescriptor.getMaxPrecision() != null;
  }

  @Override
  public boolean hasScale() {
    return fieldDescriptor.getMinScale() != null && fieldDescriptor.getMaxScale() != null;
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
