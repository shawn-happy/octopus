package com.octopus.actus.connector.jdbc.model;

import com.octopus.actus.connector.jdbc.model.dialect.doris.AggregateAlgo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnInfo {
  private String name;
  private String comment;
  private Object defaultValue;

  private FieldType fieldType;
  private Integer precision;
  private Integer scale;

  // Constraints
  private boolean nullable;
  private boolean autoIncrement;

  // for doris
  private AggregateAlgo aggregateAlgo;
}
