package io.github.octopus.sql.executor.plugin.sqlserver.model;

import com.google.common.collect.ImmutableList;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public enum SqlServerFieldType implements FieldType {
  BIT(
      "BIT",
      Types.BIT,
      FieldDescriptor.builder()
          .chName("位值类型")
          .description("bit类型，用于存储位值")
          .maxPrecision(64)
          .minPrecision(1)
          .build()),

  TINYINT(
      "TINYINT",
      Types.TINYINT,
      FieldDescriptor.builder()
          .chName("极小整数型")
          .description("tinyInt类型，有符号范围：-128 to 127，无符号范围：0 to 255")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  BOOLEAN(
      "BOOLEAN",
      Types.BOOLEAN,
      FieldDescriptor.builder()
          .chName("布尔类型")
          .description("BOOLEAN类型，相当于TINYINT(1)，0表示False，非0表示true")
          .build()),
  SMALLINT(
      "SMALLINT",
      Types.SMALLINT,
      FieldDescriptor.builder()
          .chName("短整数型")
          .description("SmallInt类型，有符号范围：-32768 to 32767，无符号范围：0 to 65535")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  INT(
      "INT",
      Types.INTEGER,
      FieldDescriptor.builder()
          .chName("整数类型")
          .description("Int类型，有符号范围：-2147483648 to 2147483647，无符号范围：0 to 4294967295")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  BIGINT(
      "BIGINT",
      Types.BIGINT,
      FieldDescriptor.builder()
          .chName("长整数型")
          .description(
              "BigInt类型，有符号范围：-9223372036854775808 to 9223372036854775807，无符号范围：0 to 18446744073709551615")
          .maxPrecision(255)
          .minPrecision(0)
          .build()),
  DECIMAL(
      "DECIMAL",
      Types.DECIMAL,
      FieldDescriptor.builder()
          .chName("数值类型")
          .description("DECIMAL类型，例如：DECIMAL(5,2)，范围：-999.99 to 999.99")
          .maxPrecision(38)
          .minPrecision(1)
          .minScale(0)
          .maxScale(38)
          .build()),
  NUMERIC(
      "NUMERIC",
      Types.NUMERIC,
      FieldDescriptor.builder()
          .chName("数字类型")
          .maxPrecision(38)
          .minPrecision(1)
          .minScale(0)
          .maxScale(38)
          .description("数字类型，精度的范围是1到38。小数位数s的范围是-84到127")
          .build()),
  FLOAT(
      "FLOAT",
      Types.FLOAT,
      FieldDescriptor.builder()
          .chName("浮点类型")
          .description("精度1-24为单精度类型，25-53为双精度类型")
          .maxPrecision(53)
          .minPrecision(0)
          .build()),
  REAL(
      "REAL",
      Types.REAL,
      FieldDescriptor.builder().chName("双精度浮点类型").description("REAL类型，相当于float(24)").build()),
  MONEY(
      "MONEY",
      Types.VARCHAR,
      FieldDescriptor.builder()
          .chName("金钱类型")
          .description("金钱类型， -922,337,203,685,477.5808 to 922,337,203,685,477.5807")
          .build()),
  SMALL_MONEY(
      "SMALLMONEY",
      Types.VARCHAR,
      FieldDescriptor.builder()
          .chName("小金钱类型")
          .description("小金钱类型， -214,748.3648 to 214,748.3647")
          .build()),

  DATE(
      "DATE",
      Types.DATE,
      FieldDescriptor.builder()
          .chName("日期类型")
          .description("Date类型，格式：YYYY-MM-DD，范围：'1000-01-01' to '9999-12-31'")
          .build()),
  DATETIME(
      "DATETIME",
      Types.TIMESTAMP,
      FieldDescriptor.builder()
          .chName("日期时间类型")
          .description(
              "DateTime类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  DATETIME2(
      "DATETIME2",
      Types.TIMESTAMP,
      FieldDescriptor.builder()
          .chName("日期时间类型2")
          .description(
              "DateTime类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  TIME(
      "TIME",
      Types.TIME,
      FieldDescriptor.builder()
          .chName("时间类型")
          .description(
              "Time类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'-838:59:59.000000' to '838:59:59.000000'")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  DATETIME_OFFSET(
      "DATETIMEOFFSET",
      Types.TIMESTAMP,
      FieldDescriptor.builder()
          .chName("日期时间偏移类型")
          .description(
              "DateTime类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  SMALL_DATETIME(
      "SMALLDATETIME",
      Types.TIMESTAMP,
      FieldDescriptor.builder()
          .chName("日期时间偏移类型")
          .description(
              "DateTime类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  VARCHAR(
      "VARCHAR",
      Types.VARCHAR,
      FieldDescriptor.builder()
          .chName("字符串类型")
          .description("Varchar类型，变长字符串")
          .maxPrecision(8000)
          .minPrecision(0)
          .build()),
  SYSNAME(
      "SYSNAME",
      Types.VARCHAR,
      FieldDescriptor.builder()
          .chName("系统名字类型")
          .description("系统名字类型")
          .maxPrecision(8000)
          .minPrecision(0)
          .build()),
  CHAR(
      "CHAR",
      Types.CHAR,
      FieldDescriptor.builder()
          .chName("字符类型")
          .description("字符类型，固定长度")
          .maxPrecision(8000)
          .minPrecision(0)
          .build()),
  NVARCHAR(
      "NVARCHAR",
      Types.NVARCHAR,
      FieldDescriptor.builder()
          .chName("Unicode字符串类型")
          .description("变长Unicode字符串")
          .maxPrecision(4000)
          .minPrecision(0)
          .build()),
  NCHAR(
      "NCHAR",
      Types.NCHAR,
      FieldDescriptor.builder()
          .chName("Unicode字符类型")
          .description("Unicode字符类型，固定长度")
          .maxPrecision(4000)
          .minPrecision(0)
          .build()),
  TEXT(
      "TEXT",
      Types.CLOB,
      FieldDescriptor.builder().chName("文本类型").description("TEXT类型，长文本字符串").build()),
  NTEXT(
      "NTEXT",
      Types.NCLOB,
      FieldDescriptor.builder().chName("文本类型").description("TEXT类型，长文本字符串").build()),

  JSON(
      "JSON",
      Types.VARCHAR,
      FieldDescriptor.builder().chName("JSON类型").description("JSON类型").build()),
  BINARY(
      "BINARY",
      Types.BINARY,
      FieldDescriptor.builder()
          .chName("BINARY类型")
          .minPrecision(0)
          .maxPrecision(8000)
          .description("与Char类型相似")
          .build()),
  VARBINARY(
      "VARBINARY",
      Types.VARBINARY,
      FieldDescriptor.builder()
          .chName("VARBINARY类型")
          .minPrecision(0)
          .maxPrecision(8000)
          .description("与VarChar类型相似")
          .build()),
  IMAGE(
      "IMAGE",
      Types.BLOB,
      FieldDescriptor.builder().chName("图片类型").description("与Char类型相似").build()),
  ;
  private final String dataType;
  private final int sqlType;
  private final FieldDescriptor fieldDescriptor;

  private static final List<FieldType> NUMERIC_TYPES =
      ImmutableList.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, FLOAT, REAL);
  private static final List<FieldType> STRING_TYPES = ImmutableList.of(VARCHAR);
  private static final List<FieldType> DATE_TIME_TYPES =
      ImmutableList.of(DATE, DATETIME, DATETIME2, DATETIME_OFFSET, SMALL_DATETIME);

  SqlServerFieldType(String dataType, int sqlType, FieldDescriptor fieldDescriptor) {
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
  public int getSqlType() {
    return sqlType;
  }

  @Override
  public String toString() {
    return String.format("oracle datatype: %s, %s", dataType, fieldDescriptor.toString());
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

  public static SqlServerFieldType of(String dataType) {
    return Arrays.stream(values())
        .filter(type -> type.getDataType().equalsIgnoreCase(dataType))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "the data type [%s] is not supported with sqlserver.", dataType)));
  }
}
