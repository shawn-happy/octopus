package com.octopus.kettlex.core.row.column;

import com.octopus.kettlex.core.exception.KettleXConvertException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import org.apache.commons.lang3.math.NumberUtils;

public class LongColumn extends Column {

  /**
   * 从整形字符串表示转为LongColumn，支持Java科学计数法
   *
   * <p>NOTE: <br>
   * 如果data为浮点类型的字符串表示，数据将会失真，请使用DoubleColumn对接浮点字符串
   */
  public LongColumn(final String data) {
    super(null, FieldType.Long);
    if (null == data) {
      return;
    }

    try {
      BigInteger rawData = NumberUtils.createBigDecimal(data).toBigInteger();
      super.setRawData(rawData);
    } catch (Exception e) {
      throw new KettleXConvertException(String.format("String[%s]不能转为Long .", data));
    }
  }

  public LongColumn(Long data) {
    this(null == data ? (BigInteger) null : BigInteger.valueOf(data));
  }

  public LongColumn(Integer data) {
    this(null == data ? (BigInteger) null : BigInteger.valueOf(data));
  }

  public LongColumn(BigInteger data) {
    this(data, null == data ? 0 : 8);
  }

  private LongColumn(BigInteger data, int byteSize) {
    super(data, FieldType.Long);
  }

  public LongColumn() {
    this((BigInteger) null);
  }

  @Override
  public BigInteger asBigInteger() {
    if (null == this.getRawData()) {
      return null;
    }

    return (BigInteger) this.getRawData();
  }

  @Override
  public Long asLong() {
    BigInteger rawData = (BigInteger) this.getRawData();
    if (null == rawData) {
      return null;
    }

    OverFlowUtil.validateLongNotOverFlow(rawData);

    return rawData.longValue();
  }

  @Override
  public Double asDouble() {
    if (null == this.getRawData()) {
      return null;
    }

    BigDecimal decimal = this.asBigDecimal();
    OverFlowUtil.validateDoubleNotOverFlow(decimal);

    return decimal.doubleValue();
  }

  @Override
  public Boolean asBoolean() {
    if (null == this.getRawData()) {
      return null;
    }

    return this.asBigInteger().compareTo(BigInteger.ZERO) != 0 ? true : false;
  }

  @Override
  public BigDecimal asBigDecimal() {
    if (null == this.getRawData()) {
      return null;
    }

    return new BigDecimal(this.asBigInteger());
  }

  @Override
  public String asString() {
    if (null == this.getRawData()) {
      return null;
    }
    return ((BigInteger) this.getRawData()).toString();
  }

  @Override
  public Date asDate() {
    if (null == this.getRawData()) {
      return null;
    }
    return new Date(this.asLong());
  }

  @Override
  public Date asDate(String dateFormat) {
    return this.asDate();
  }
}
