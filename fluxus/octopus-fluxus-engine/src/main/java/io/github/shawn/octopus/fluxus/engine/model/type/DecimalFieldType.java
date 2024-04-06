package io.github.shawn.octopus.fluxus.engine.model.type;

import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.math.BigDecimal;
import lombok.Getter;

@Getter
public class DecimalFieldType implements DataWorkflowFieldType {

  private final int precision;
  private final int scale;

  public DecimalFieldType(int precision, int scale) {
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
