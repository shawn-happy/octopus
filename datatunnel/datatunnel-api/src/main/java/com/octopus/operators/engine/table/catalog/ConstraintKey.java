package com.octopus.operators.engine.table.catalog;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class ConstraintKey {

  private final ConstraintType constraintType;
  private final String constraintName;
  private final List<ConstraintKeyColumn> columnNames;
}
