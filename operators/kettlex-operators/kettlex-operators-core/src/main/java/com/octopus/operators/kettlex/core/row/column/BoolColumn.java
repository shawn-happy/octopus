package com.octopus.operators.kettlex.core.row.column;

import com.octopus.operators.kettlex.core.exception.KettleXConvertException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class BoolColumn extends Column {
  public BoolColumn(Boolean bool) {
    super(bool, FieldType.Boolean);
  }

  public BoolColumn(final String data) {
    this(true);
    this.validate(data);
    if (null == data) {
      this.setRawData(null);
    } else {
      this.setRawData(Boolean.valueOf(data));
    }
    return;
  }

  public BoolColumn() {
    super(null, FieldType.Boolean);
  }

  @Override
  public Boolean asBoolean() {
    if (null == super.getRawData()) {
      return null;
    }

    return (Boolean) super.getRawData();
  }

  @Override
  public Long asLong() {
    if (null == this.getRawData()) {
      return null;
    }

    return this.asBoolean() ? 1L : 0L;
  }

  @Override
  public Double asDouble() {
    if (null == this.getRawData()) {
      return null;
    }

    return this.asBoolean() ? 1.0d : 0.0d;
  }

  @Override
  public String asString() {
    if (null == super.getRawData()) {
      return null;
    }

    return this.asBoolean() ? "true" : "false";
  }

  @Override
  public BigInteger asBigInteger() {
    if (null == this.getRawData()) {
      return null;
    }

    return BigInteger.valueOf(this.asLong());
  }

  @Override
  public BigDecimal asBigDecimal() {
    if (null == this.getRawData()) {
      return null;
    }

    return BigDecimal.valueOf(this.asLong());
  }

  private void validate(final String data) {
    if (null == data) {
      return;
    }

    if ("true".equalsIgnoreCase(data) || "false".equalsIgnoreCase(data)) {
      return;
    }

    throw new KettleXConvertException(String.format("String[%s] cannot cast Boolean .", data));
  }
}
