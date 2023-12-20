package com.octopus.operators.engine.table.catalog;

import com.octopus.operators.engine.table.type.RowDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Column {
  private String name;
  private RowDataType type;
  private int length;
  private boolean nullable;
  private Object defaultValue;
  private String comment;
}
