package io.github.octopus.actus.core.model.schema;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class IndexDefinition {
  private String name;
  private List<String> columns;
  private String comment;
  private IndexAlgo indexAlgo;
}
