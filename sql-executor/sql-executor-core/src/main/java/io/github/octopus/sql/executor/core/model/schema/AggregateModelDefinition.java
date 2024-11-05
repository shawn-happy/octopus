package io.github.octopus.sql.executor.core.model.schema;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AggregateModelDefinition {
  private AggregateModelType type;
  private List<String> columns;
}