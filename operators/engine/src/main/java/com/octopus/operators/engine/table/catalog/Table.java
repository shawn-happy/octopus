package com.octopus.operators.engine.table.catalog;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@AllArgsConstructor
public class Table {

  private final String catalogName;
  private final String databaseName;
  private final String tableName;
  private final List<Column> columns;
  private final PrimaryKey primaryKey;
  private final List<ConstraintKey> constraintKeys;
}
