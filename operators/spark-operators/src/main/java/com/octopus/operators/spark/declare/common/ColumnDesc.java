package com.octopus.operators.spark.declare.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ColumnDesc {
  private String name;
  private String alias;
  private FieldType type;
  private boolean nullable;
  private boolean primaryKey;

  public String getSelectExpr() {
    if (StringUtils.isBlank(alias)) {
      return null;
    }
    return String.format("%s as %s", name, alias);
  }
}
