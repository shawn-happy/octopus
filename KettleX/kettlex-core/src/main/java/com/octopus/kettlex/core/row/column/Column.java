package com.octopus.kettlex.core.row.column;

import com.octopus.kettlex.core.exception.KettleXConvertException;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.row.column.DateColumn.DateType;
import com.octopus.kettlex.core.utils.JsonUtil;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public abstract class Column {

  private String name;
  private FieldType type;
  private Object rawData;

  public Column(Object rawData, FieldType type) {
    this.rawData = rawData;
    this.type = type;
  }

  public Column(Object rawData, FieldType type, String name) {
    this.rawData = rawData;
    this.type = type;
    this.name = name;
  }

  public Object getRawData() {
    return this.rawData;
  }

  public String getName() {
    return name;
  }

  public FieldType getType() {
    return this.type;
  }

  protected void setType(FieldType type) {
    this.type = type;
  }

  protected void setRawData(Object rawData) {
    this.rawData = rawData;
  }

  protected void setName(String name) {
    this.name = name;
  }

  public Long asLong() {
    throw new KettleXConvertException(String.format("%s cannot cast Long", type.name()));
  }

  public Double asDouble() {
    throw new KettleXConvertException(String.format("%s cannot cast Double", type.name()));
  }

  public String asString() {
    throw new KettleXConvertException(String.format("%s cannot cast String", type.name()));
  }

  public Date asDate() {
    throw new KettleXConvertException(String.format("%s cannot cast Date", type.name()));
  }

  public Date asDate(String dateFormat) {
    throw new KettleXConvertException(
        String.format("%s cannot cast Date by format: [%s]", type.name(), dateFormat));
  }

  public byte[] asBytes() {
    throw new KettleXConvertException(String.format("%s cannot cast bytes", type.name()));
  }

  public Boolean asBoolean() {
    throw new KettleXConvertException(String.format("%s cannot cast Boolean", type.name()));
  }

  public BigDecimal asBigDecimal() {
    throw new KettleXConvertException(String.format("%s cannot cast BigDecimal", type.name()));
  }

  public BigInteger asBigInteger() {
    throw new KettleXConvertException(String.format("%s cannot cast BigInteger", type.name()));
  }

  @Override
  public String toString() {
    return JsonUtil.toJson(this).orElse(null);
  }

  public static ColumnBuilder builder() {
    return new ColumnBuilder();
  }

  public static class ColumnBuilder {
    private FieldType type;
    private String name;
    private Object rawData;

    public ColumnBuilder name(String name) {
      this.name = name;
      return this;
    }

    public ColumnBuilder type(FieldType type) {
      this.type = type;
      return this;
    }

    public ColumnBuilder rawData(Object rawData) {
      this.rawData = rawData;
      return this;
    }

    public Column build() {
      Column col = null;
      switch (type) {
        case Boolean:
          col = new BoolColumn();
          break;
        case Byte:
        case Int:
        case Long:
          col = new LongColumn();
          break;
        case Float:
        case Double:
          col = new DoubleColumn();
          break;
        case String:
          col = new StringColumn();
          break;
        case Date:
          DateColumn dateColumn = new DateColumn();
          dateColumn.setSubType(DateType.DATE);
          col = dateColumn;
          break;
        case DateTime:
          DateColumn dateTimeColumn = new DateColumn();
          dateTimeColumn.setSubType(DateType.DATETIME);
          col = dateTimeColumn;
          break;
        case Timestamp:
          DateColumn timestampColumn = new DateColumn();
          timestampColumn.setSubType(DateType.TIMESTEMP);
          col = timestampColumn;
          break;
        default:
          throw new KettleXException("Unsupported Column type: " + type);
      }
      col.setType(type);
      col.setName(name);
      col.setRawData(rawData);
      return col;
    }
  }
}
