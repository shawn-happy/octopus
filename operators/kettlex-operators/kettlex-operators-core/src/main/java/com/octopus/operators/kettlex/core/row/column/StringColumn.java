package com.octopus.operators.kettlex.core.row.column;

import com.octopus.operators.kettlex.core.exception.KettleXConvertException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.TimeZone;
import org.apache.commons.lang3.time.FastDateFormat;

public class StringColumn extends Column {

  public StringColumn() {
    this((String) null);
  }

  public StringColumn(final String rawData) {
    super(rawData, FieldType.String);
  }

  @Override
  public String asString() {
    if (null == this.getRawData()) {
      return null;
    }

    return (String) this.getRawData();
  }

  private void validateDoubleSpecific(final String data) {
    if ("NaN".equals(data) || "Infinity".equals(data) || "-Infinity".equals(data)) {
      throw new KettleXConvertException(
          String.format("String[\"%s\"]属于Double特殊类型，不能转为其他类型 .", data));
    }

    return;
  }

  @Override
  public BigInteger asBigInteger() {
    if (null == this.getRawData()) {
      return null;
    }

    this.validateDoubleSpecific((String) this.getRawData());

    try {
      return this.asBigDecimal().toBigInteger();
    } catch (Exception e) {
      throw new KettleXConvertException(
          String.format("String[\"%s\"]不能转为BigInteger .", this.asString()));
    }
  }

  @Override
  public Long asLong() {
    if (null == this.getRawData()) {
      return null;
    }

    this.validateDoubleSpecific((String) this.getRawData());

    try {
      BigInteger integer = this.asBigInteger();
      OverFlowUtil.validateLongNotOverFlow(integer);
      return integer.longValue();
    } catch (Exception e) {
      throw new KettleXConvertException(String.format("String[\"%s\"]不能转为Long .", this.asString()));
    }
  }

  @Override
  public BigDecimal asBigDecimal() {
    if (null == this.getRawData()) {
      return null;
    }

    this.validateDoubleSpecific((String) this.getRawData());

    try {
      return new BigDecimal(this.asString());
    } catch (Exception e) {
      throw new KettleXConvertException(
          String.format("String [\"%s\"] 不能转为BigDecimal .", this.asString()));
    }
  }

  @Override
  public Double asDouble() {
    if (null == this.getRawData()) {
      return null;
    }

    String data = (String) this.getRawData();
    if ("NaN".equals(data)) {
      return Double.NaN;
    }

    if ("Infinity".equals(data)) {
      return Double.POSITIVE_INFINITY;
    }

    if ("-Infinity".equals(data)) {
      return Double.NEGATIVE_INFINITY;
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

    if ("true".equalsIgnoreCase(this.asString())) {
      return true;
    }

    if ("false".equalsIgnoreCase(this.asString())) {
      return false;
    }

    throw new KettleXConvertException(String.format("String[\"%s\"]不能转为Bool .", this.asString()));
  }

  @Override
  public Date asDate() {
    try {
      return FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getDefault()).parse(asString());
    } catch (Exception e) {
      throw new KettleXConvertException(String.format("String[\"%s\"]不能转为Date .", this.asString()));
    }
  }

  @Override
  public Date asDate(String dateFormat) {
    try {
      return FastDateFormat.getInstance(dateFormat, TimeZone.getDefault()).parse(asString());
    } catch (Exception e) {
      throw new KettleXConvertException(String.format("String[\"%s\"]不能转为Date .", this.asString()));
    }
  }

  @Override
  public byte[] asBytes() {
    try {
      if (null == asString()) {
        return null;
      }

      return asString().getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new KettleXConvertException(
          String.format("String[\"%s\"]不能转为Bytes .", this.asString()));
    }
  }
}