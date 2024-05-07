package com.octopus.operators.engine.table.catalog;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@AllArgsConstructor
public class ConstraintKeyColumn {

  private final String columnName;
  private final ColumnSortType sortType;
}
