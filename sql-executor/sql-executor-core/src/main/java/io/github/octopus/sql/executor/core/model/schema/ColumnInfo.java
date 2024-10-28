package io.github.octopus.sql.executor.core.model.schema;

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
  @Builder.Default private boolean nullable = true;
  private boolean autoIncrement;
  private boolean uniqueKey;
  private boolean primaryKey;
}
