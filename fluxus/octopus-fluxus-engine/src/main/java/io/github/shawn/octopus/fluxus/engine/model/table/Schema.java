package io.github.shawn.octopus.fluxus.engine.model.table;

import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public final class Schema {
  private String fieldName;
  private DataWorkflowFieldType fieldType;
  private Integer precision;
  private Integer scale;
  private boolean nullable;
  private Object defaultValue;
  private String comment;
}
