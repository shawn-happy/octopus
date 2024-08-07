package io.github.octopus.datos.centro.sql.executor.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Column {
  private String columnName;
  private String fieldType;
  private String comment;
  private boolean nullable;
  private Object defaultValue;
  private boolean autoIncrement;

  // for doris
  private String aggregateType;
}
