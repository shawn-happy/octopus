package io.github.octopus.sql.executor.core.model.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ColumnDefinition {
  private String column;
  private FieldType fieldType;
  private Integer precision;
  private Integer scale;
  @Setter private String comment;
  private Object defaultValue;
  private boolean nullable;
  private boolean autoIncrement;

  // for doris
  private AggregateAlgo aggregateAlgo;
}
