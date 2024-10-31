package io.github.octopus.sql.executor.plugin.doris.model;

import com.google.common.collect.ImmutableList;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public enum DorisFieldType implements FieldType {
  TINYINT(
      "TINYINT", Types.TINYINT,
      FieldDescriptor.builder()
          .chName("极小整数型")
          .description("tinyInt类型，范围：-128 to 127")
          .minPrecision(0)
          .maxPrecision(4)
          .build()),
  BOOLEAN(
      "BOOLEAN", Types.BOOLEAN,
      FieldDescriptor.builder()
          .chName("布尔类型")
          .description("BOOLEAN类型，相当于TINYINT(1)，0表示False，非0表示true")
          .build()),
  SMALLINT(
      "SMALLINT", Types.SMALLINT,
      FieldDescriptor.builder()
          .chName("短整数型")
          .description("SmallInt类型，范围：-32768 to 32767")
          .minPrecision(0)
          .maxPrecision(6)
          .build()),
  INT(
      "INT", Types.INTEGER,
      FieldDescriptor.builder()
          .chName("整数类型")
          .description("Int类型，范围：-2147483648 to 2147483647")
          .minPrecision(0)
          .maxPrecision(11)
          .build()),
  BIGINT(
      "BIGINT", Types.BIGINT,
      FieldDescriptor.builder()
          .chName("长整数型")
          .description("BigInt类型，范围：-9223372036854775808 to 9223372036854775807")
          .minPrecision(0)
          .maxPrecision(20)
          .build()),
  LARGEINT(
      "LARGEINT", Types.BIGINT,
      FieldDescriptor.builder()
          .chName("巨大整型")
          .description("LargeInt类型，范围[-2^127 + 1 ~ 2^127 - 1]")
          .minPrecision(0)
          .maxPrecision(40)
          .build()),
  DECIMAL(
      "DECIMAL", Types.DECIMAL,
      FieldDescriptor.builder()
          .chName("数值类型")
          .description("DECIMAL类型，例如：DECIMAL(5,2)，范围：-999.99 to 999.99")
          .minPrecision(1)
          .maxPrecision(38)
          .minScale(0)
          .maxScale(38)
          .build()),
  FLOAT("FLOAT", Types.FLOAT,
      FieldDescriptor.builder().chName("单精度浮点类型").description("Float类型，单精度").build()),
  DOUBLE("DOUBLE", Types.DOUBLE,
      FieldDescriptor.builder().chName("双精度浮点类型").description("Double类型，双精度").build()),
  DATE(
      "DATE", Types.DATE,
      FieldDescriptor.builder()
          .chName("日期类型")
          .description("Date类型，格式：YYYY-MM-DD，范围：'0000-01-01' to '9999-12-31'")
          .build()),
  DATETIME(
      "DATETIME", Types.TIMESTAMP,
      FieldDescriptor.builder()
          .chName("日期时间类型")
          .description(
              "DateTime类型，格式：YYYY-MM-DD hh:mm:ss[.fraction]，范围：'0000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'")
          .maxPrecision(6)
          .minPrecision(0)
          .build()),
  VARCHAR(
      "VARCHAR", Types.VARCHAR,
      FieldDescriptor.builder()
          .chName("字符串类型")
          .description("Varchar类型，变长字符串")
          .maxPrecision(65535)
          .minPrecision(1)
          .build()),
  CHAR(
      "CHAR", Types.CHAR,
      FieldDescriptor.builder()
          .chName("字符类型")
          .description("字符类型，定长字符")
          .maxPrecision(65535)
          .minPrecision(1)
          .build()),
  STRING("STRING", Types.VARCHAR,
      FieldDescriptor.builder().chName("文本类型").description("类似与Varchar").build()),
  ARRAY(
      "ARRAY", Types.ARRAY,
      FieldDescriptor.builder()
          .chName("数组类型")
          .description("由 T类型元素组成的数组，不能作为Key列使用。目前支持在Duplicate和Unique模型的表中使用")
          .build()),
  MAP(
      "MAP", Types.JAVA_OBJECT,
      FieldDescriptor.builder()
          .chName("键值类型")
          .description("由K, V类型元素组成的键值对，不能作为Key列使用。目前支持在Duplicate和Unique模型的表中使用")
          .build()),
  STRUCT(
      "STRUCT", Types.JAVA_OBJECT,
      FieldDescriptor.builder()
          .chName("结构类型")
          .description("由多个Field组成的结构体，不能作为Key列使用。目前支持在Duplicate模型的表中使用")
          .build()),
  JSON(
      "JSON", Types.VARCHAR,
      FieldDescriptor.builder()
          .chName("JSON类型")
          .description("采用二进制JSON格式存储，通过JSON函数访问JSON内部字段")
          .build()),
  VARIANT(
      "VARIANT", Types.JAVA_OBJECT,
      FieldDescriptor.builder()
          .chName("动态可变数据类型")
          .description("专为半结构化数据设计，提升存储效率和查询性能，只能用作Value列")
          .build()),
  HLL(
      "HLL", Types.JAVA_OBJECT,
      FieldDescriptor.builder()
          .chName("基数统计类型")
          .description("HLL是模糊去重，在数据量大的情况性能优于Count Distinct")
          .build()),
  BITMAP(
      "BITMAP", Types.JAVA_OBJECT,
      FieldDescriptor.builder()
          .chName("位图类型")
          .description("Bitmap类型的列可以在Aggregate表、Unique表或Duplicate表中使用")
          .build()),
  QUANTILE_STATE(
      "QUANTILE_STATE",Types.JAVA_OBJECT,
      FieldDescriptor.builder()
          .chName("近似值类型")
          .description("QUANTILE_STATE不能作为key列使用，支持在Aggregate模型、Duplicate模型和 Unique模型的表中使用")
          .build()),
  AGG_STATE(
      "QUANTILE_STATE", Types.JAVA_OBJECT,
      FieldDescriptor.builder()
          .chName("聚合类型")
          .description("AGG_STATE不能作为Key列使用，建表时需要同时声明聚合函数的签名")
          .build()),
  ;
  private final String dataType;
  private final int sqlType;
  private final FieldDescriptor fieldDescriptor;

  private static final List<FieldType> NUMERIC_TYPES =
      ImmutableList.of(TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DECIMAL, FLOAT, DOUBLE);
  private static final List<FieldType> STRING_TYPES = ImmutableList.of(VARCHAR);
  private static final List<FieldType> DATE_TIME_TYPES = ImmutableList.of(DATE, DATETIME);

  // Doris里即使你设置了长度，也不生效的类型，例如Int(2) -> 在Columns表显示的还是Int(11)， 所以这种情况，我们也认为没有长度
  private static final List<FieldType> SET_LENGTH_NOT_EFFECT_TYPES =
      ImmutableList.of(TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE);

  DorisFieldType(String dataType, int sqlType, FieldDescriptor fieldDescriptor) {
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
  public int getSqlType() {
    return sqlType;
  }

  @Override
  public boolean hasPrecision() {
    return fieldDescriptor.getMinPrecision() != null
        && fieldDescriptor.getMaxPrecision() != null
        && !SET_LENGTH_NOT_EFFECT_TYPES.contains(this);
  }

  @Override
  public boolean hasScale() {
    return fieldDescriptor.getMinScale() != null && fieldDescriptor.getMaxScale() != null;
  }

  @Override
  public String toString() {
    return String.format("doris datatype: %s,  %s", dataType, fieldDescriptor.toString());
  }

  public static DorisFieldType of(String dataType) {
    if ("bigint unsigned".equalsIgnoreCase(dataType)) {
      return LARGEINT;
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
