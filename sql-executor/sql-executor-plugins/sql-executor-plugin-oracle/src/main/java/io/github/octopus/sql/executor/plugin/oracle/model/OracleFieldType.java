package io.github.octopus.sql.executor.plugin.oracle.model;

import com.google.common.collect.ImmutableList;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public enum OracleFieldType implements FieldType {
  VARCHAR2(
      "VARCHAR2",
      Types.VARCHAR,
      FieldDescriptor.builder().chName("字符串类型").description("可变字符串类型").build()),
  NVARCHAR2(
      "NVARCHAR2",
      Types.NVARCHAR,
      FieldDescriptor.builder().chName("Unicode类型").description("Unicode字符串类型").build()),
  NUMBER(
      "NUMBER",
      Types.NUMERIC,
      FieldDescriptor.builder()
          .chName("数字类型")
          .maxPrecision(38)
          .minPrecision(1)
          .minScale(-84)
          .maxScale(127)
          .description("数字类型，精度的范围是1到38。小数位数s的范围是-84到127")
          .build()),
  FLOAT(
      "FLOAT",
      Types.FLOAT,
      FieldDescriptor.builder()
          .chName("数字子类型")
          .minPrecision(1)
          .maxPrecision(126)
          .description("数字子类型，只有精度，精度范围")
          .build()),
  LONG(
      "LONG", Types.BIGINT, FieldDescriptor.builder().chName("长文本类型").description("长文本类型").build()),
  BINARY_FLOAT(
      "BINARY_FLOAT",
      Types.BINARY,
      FieldDescriptor.builder().chName("单精度浮点类型").description("单精度浮点类型").build()),
  BINARY_DOUBLE(
      "BINARY_DOUBLE",
      Types.BINARY,
      FieldDescriptor.builder().chName("双精度浮点类型").description("双精度浮点类型").build()),
  DATE(
      "DATE",
      Types.DATE,
      FieldDescriptor.builder()
          .chName("时间类型")
          .description("有效日期范围从公元前4712年1月1日到公元9999年12月31日")
          .build()),

  TIMESTAMP(
      "TIMESTAMP",
      Types.TIMESTAMP,
      FieldDescriptor.builder()
          .chName("时间戳类型")
          .minPrecision(0)
          .maxPrecision(9)
          .description("时间戳类型，可以表示年月日时分秒，精度表示的是秒的长度")
          .build()),
  TIMESTAMP_WITH_TIME_ZONE(
      "TIMESTAMP WITH TIME ZONE",
      Types.TIMESTAMP_WITH_TIMEZONE,
      FieldDescriptor.builder()
          .chName("时间戳时区类型")
          .minPrecision(0)
          .maxPrecision(9)
          .description("时间戳时区类型，可以表示年月日时分秒，精度表示的是秒的长度")
          .build()),

  TIMESTAMP_WITH_LOCAL_TIME_ZONE(
      "TIMESTAMP WITH LOCAL TIME ZONE",
      Types.TIMESTAMP_WITH_TIMEZONE,
      FieldDescriptor.builder()
          .chName("时间戳本地时区类型")
          .minPrecision(0)
          .maxPrecision(9)
          .description("时间戳时区类型，可以表示年月日时分秒，精度表示的是秒的长度")
          .build()),
  ROWID(
      "ROWID", Types.ROWID, FieldDescriptor.builder().chName("行ID类型").description("行ID类型").build()),
  CHAR("CHAR", Types.CHAR, FieldDescriptor.builder().chName("字符类型").description("字符类型").build()),
  NCHAR(
      "NCHAR",
      Types.NCHAR,
      FieldDescriptor.builder().chName("unicode字符类型").description("unicode字符类型").build()),
  CLOB("CLOB", Types.CLOB, FieldDescriptor.builder().chName("文本类型").description("文本类型").build()),
  NCLOB(
      "NCLOB",
      Types.NCLOB,
      FieldDescriptor.builder().chName("unicode文本类型").description("unicode文本类型").build()),
  BLOB("BLOB", Types.BLOB, FieldDescriptor.builder().chName("二进制类型").description("二进制类型").build()),
  ;

  private final String dataType;
  private final int sqlType;
  private final FieldDescriptor fieldDescriptor;

  private static final List<FieldType> NUMERIC_TYPES =
      ImmutableList.of(NUMBER, FLOAT, BINARY_FLOAT, BINARY_DOUBLE);
  private static final List<FieldType> STRING_TYPES =
      ImmutableList.of(VARCHAR2, NVARCHAR2, CHAR, NCHAR, NCLOB, CLOB, ROWID, LONG);
  private static final List<FieldType> DATE_TIME_TYPES =
      ImmutableList.of(DATE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE);

  OracleFieldType(String dataType, int sqlType, FieldDescriptor fieldDescriptor) {
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
  public int getSqlType() {
    return sqlType;
  }

  @Override
  public String getDescription() {
    return fieldDescriptor.getDescription();
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

  public static OracleFieldType of(String dataType) {
    return Arrays.stream(values())
        .filter(type -> type.getDataType().equalsIgnoreCase(dataType))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("the data type [%s] is not supported with oracle.", dataType)));
  }
}
