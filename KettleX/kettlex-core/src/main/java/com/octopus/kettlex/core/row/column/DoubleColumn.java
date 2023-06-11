package com.octopus.kettlex.core.row.column;

import com.octopus.kettlex.core.exception.KettleXConvertException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class DoubleColumn extends Column {

  public DoubleColumn(final String data) {
    this(data, null == data ? 0 : data.length());
    this.validate(data);
  }

  public DoubleColumn(Long data) {
    this(data == null ? (String) null : String.valueOf(data));
  }

  public DoubleColumn(Integer data) {
    this(data == null ? (String) null : String.valueOf(data));
  }

  /** Double无法表示准确的小数数据，我们不推荐使用该方法保存Double数据，建议使用String作为构造入参 */
  public DoubleColumn(final Double data) {
    this(data == null ? (String) null : new BigDecimal(String.valueOf(data)).toPlainString());
  }

  /** Float无法表示准确的小数数据，我们不推荐使用该方法保存Float数据，建议使用String作为构造入参 */
  public DoubleColumn(final Float data) {
    this(data == null ? (String) null : new BigDecimal(String.valueOf(data)).toPlainString());
  }

  public DoubleColumn(final BigDecimal data) {
    this(null == data ? (String) null : data.toPlainString());
  }

  public DoubleColumn(final BigInteger data) {
    this(null == data ? (String) null : data.toString());
  }

  public DoubleColumn() {
    this((String) null);
  }

  private DoubleColumn(final String data, int byteSize) {
    super(data, FieldType.Double);
  }

  @Override
  public BigDecimal asBigDecimal() {
    if (null == this.getRawData()) {
      return null;
    }

    try {
      return new BigDecimal((String) this.getRawData());
    } catch (NumberFormatException e) {
      throw new KettleXConvertException(
          String.format("String[%s] 无法转换为Double类型 .", (String) this.getRawData()));
    }
  }

  @Override
  public Double asDouble() {
    if (null == this.getRawData()) {
      return null;
    }

    String string = (String) this.getRawData();

    boolean isDoubleSpecific =
        string.equals("NaN") || string.equals("-Infinity") || string.equals("+Infinity");
    if (isDoubleSpecific) {
      return Double.valueOf(string);
    }

    BigDecimal result = this.asBigDecimal();
    OverFlowUtil.validateDoubleNotOverFlow(result);

    return result.doubleValue();
  }

  @Override
  public Long asLong() {
    if (null == this.getRawData()) {
      return null;
    }

    BigDecimal result = this.asBigDecimal();
    OverFlowUtil.validateLongNotOverFlow(result.toBigInteger());

    return result.longValue();
  }

  @Override
  public BigInteger asBigInteger() {
    if (null == this.getRawData()) {
      return null;
    }

    return this.asBigDecimal().toBigInteger();
  }

  @Override
  public String asString() {
    if (null == this.getRawData()) {
      return null;
    }
    return (String) this.getRawData();
  }

  private void validate(final String data) {
    if (null == data) {
      return;
    }

    if (data.equalsIgnoreCase("NaN")
        || data.equalsIgnoreCase("-Infinity")
        || data.equalsIgnoreCase("Infinity")) {
      return;
    }

    try {
      new BigDecimal(data);
    } catch (Exception e) {
      throw new KettleXConvertException(String.format("String[%s]无法转为Double类型 .", data));
    }
  }
}
