package com.octopus.operators.engine.table.type;

import java.math.BigDecimal;
import lombok.Getter;

public class DecimalDataType implements RowDataType {

  @Getter private final int precision;

  @Getter private final int scale;

  public DecimalDataType(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public Class<?> getTypeClass() {
    return BigDecimal.class;
  }

  @Override
  public String toString() {
    return String.format("decimal<%d, %d>", precision, scale);
  }
}
